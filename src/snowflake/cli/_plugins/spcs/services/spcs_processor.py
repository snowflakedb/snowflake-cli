import logging
import os
from pathlib import Path
import time
from typing import List

from snowflake.cli._plugins.nativeapp.artifacts import build_bundle
from snowflake.cli._plugins.spcs.image_registry.manager import RegistryManager
from snowflake.cli._plugins.spcs.image_registry.registry import Registry
from snowflake.cli._plugins.spcs.image_repository.manager import ImageRepositoryManager
from snowflake.cli._plugins.spcs.services.manager import ServiceManager
from snowflake.cli._plugins.spcs.services.project_model import ServiceProjectModel
from snowflake.cli._plugins.stage.diff import DiffResult
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.entities.utils import sync_deploy_root_with_stage
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.schemas.v1.spcs.service import Service
from snowflake.cli.api.project.util import extract_database, extract_schema

logger = logging.getLogger(__name__)


class SpcsProcessor(ServiceManager):
    def __init__(self, project_definition: Service, project_root: Path):
        self._project_definition = ServiceProjectModel(project_definition, project_root)
        self._registry_manager = RegistryManager()
        self._repo_manager = ImageRepositoryManager()
        self._service_manager = ServiceManager()

    def deploy(
        self,
        prune: bool = True,
        recursive: bool = True,
        paths: List[Path] = None,
        print_diff: bool = True,
    ):
        # 1. Upload service spec and source code from deploy root local folder to stage
        artifacts = [self._project_definition.spec]
        artifacts.extend(self._project_definition.image_sources)

        stage_fqn = self._project_definition.source_stage_fqn
        stage_schema = extract_schema(stage_fqn)
        stage_database = extract_database(stage_fqn)

        diff = sync_deploy_root_with_stage(
            console=cc,
            deploy_root=self._project_definition.deploy_root,
            package_name=stage_database,
            stage_schema=stage_schema,
            bundle_map=build_bundle(
                self._project_definition.project_root,
                self._project_definition.deploy_root,
                artifacts=artifacts,
            ),
            role="SYSADMIN",  # TODO: Use the correct role
            prune=prune,
            recursive=recursive,
            stage_fqn=stage_fqn,
            local_paths_to_sync=paths,  # sync all
            print_diff=print_diff,
        )

        # 2. Rebuild images if source code has changed
        self._sync_images()

        # 3. Deploy service
        spec_path = self._project_definition.deploy_root.joinpath(
            self._project_definition.spec.src
        )
        print("Deploying service with spec " + str(spec_path))
        res = self._service_manager.create(
            service_name=self._project_definition.service_name,
            compute_pool=self._project_definition.compute_pool,
            spec_path=spec_path,
            min_instances=self._project_definition.min_instances,
            max_instances=self._project_definition.max_instances,
            query_warehouse=self._project_definition.query_warehouse,
            comment=self._project_definition.comment,
            auto_resume=True,
            external_access_integrations=None,
            tags=None,
            if_not_exists=True,
        ).fetchone()
        print(str(res[0]))

        if diff and diff.has_changes():
            print("Source change detected. Upgrading service.")
            self._service_manager.upgrade_spec(
                self._project_definition.service_name, spec_path
            )

        status = self._service_manager.status(
            self._project_definition.service_name
        ).fetchone()[1]
        retry = 10
        while status != "RUNNING" and retry > 0:
            print("Waiting for service to be ready... Status: " + status)
            time.sleep(2)
            retry -= 1
            status = self._service_manager.status(
                self._project_definition.service_name
            ).fetchone()[1]
        return self._service_manager.status(self._project_definition.service_name)

    def _sync_images(self):
        repo_fqn = FQN.from_string(self._project_definition.source_repo_fqn)

        self._repo_manager.create(
            name=repo_fqn.identifier, if_not_exists=True, replace=False
        )
        repo_url = self._repo_manager.get_repository_url(
            repo_name=repo_fqn.identifier, with_scheme=False
        )

        registry_url = self._registry_manager.get_registry_url_from_repo(repo_url)
        self._registry_manager.login_to_registry("https://" + registry_url)
        registry = Registry(registry_url, logger)

        print("Syncing images in repository " + repo_url)
        for image in self._project_definition.images:
            # image_src = self._project_definition.deploy_root.joinpath(
            #     image.src.strip("*").strip("/")
            # )
            image_src = image.src.strip("*").strip("/")
            print("image_src: " + str(image_src))

            image_src_folder_name = os.path.basename(image_src)
            image_path = os.path.join(
                self._project_definition.source_repo_path, image_src_folder_name
            )
            if image.dest:
                image_path = os.path.join(
                    self._project_definition.source_repo_path,
                    image.dest,
                    image_src_folder_name,
                )

            registry.build_and_push_image(
                image_src, image_path, "latest", False
            )  # TODO: fix tag
