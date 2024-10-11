import json
import os
import subprocess
import uuid
import datetime

import docker

TARGET_ARCH = "amd64"
DOCKER_BUILDER = "snowcli-builder"


class Registry:
    def __init__(self, registry_url, logger) -> None:
        self._registry_url = registry_url
        self._logger = logger
        self._docker_client = docker.from_env(timeout=300)
        self._is_arm = self._is_arch_arm()
        if self._is_arm:
            if os.system(f"docker buildx use {DOCKER_BUILDER}") != 0:
                os.system(f"docker buildx create --name {DOCKER_BUILDER} --use")

    def _is_arch_arm(self):
        result = subprocess.run(["uname", "-m"], stdout=subprocess.PIPE)
        arch = result.stdout.strip().decode("UTF-8")
        self._logger.info(f"Detected machine architecture: {arch}")
        return arch == "arm64" or arch == "aarch64"

    def _raise_error_from_output(self, output: str):
        for line in output.splitlines():
            try:
                jsline = json.loads(line)
                if "error" in jsline:
                    raise docker.errors.APIError(jsline["error"])
            except json.JSONDecodeError:
                pass  # not a json, don't parse, assume no error

    def _gen_image_tag(self) -> str:
        ts = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        uid = str(uuid.uuid4()).split("-")[0]
        return f"{ts}-{uid}"

    def push(self, image_name):
        self._logger.info(f"Uploading image {image_name}")
        output = self._docker_client.images.push(image_name)
        self._raise_error_from_output(output)
        return output

    def pull(self, image_name):
        self._logger.info(f"Pulling image {image_name}")

        n = image_name.rindex(":")
        if n >= 0 and "/" not in image_name[n + 1 :]:
            # if ':' is present in the last part in image name (separated by '/')
            image = image_name[0:n]
            tag = image_name[n + 1 :]
        else:
            image = image_name
            tag = None
        return self._docker_client.images.pull(image, tag, platform=TARGET_ARCH)

    def build_and_push_image(
        self,
        image_source_local_path: str,
        image_path: str,
        tag: str = None,
        generate_tag: bool = False,
    ):
        """
        builds an image and push it to sf image registry
        """

        docker_file_path = os.path.join(image_source_local_path, "Dockerfile")

        if tag is None and generate_tag:
            tag = self._gen_image_tag()

        # build and upload image to registry if running remotely
        self._logger.info("registry: " + self._registry_url)
        tagged = self._registry_url + image_path
        if tag is not None:
            tagged = f"{tagged}:{tag}"

        if self._is_arm:
            self._logger.info(f"Using docker buildx for building image {tagged}")

            docker_build_cmd = f"""
                docker buildx build --tag {tagged}
                --load
                --platform linux/amd64
                {image_source_local_path}
                -f {docker_file_path}
                --builder {DOCKER_BUILDER}
                --rm
                """

            parts = list(
                filter(
                    lambda part: part != "",
                    [part.strip() for part in docker_build_cmd.split("\n")],
                )
            )
            docker_cmd = " ".join(parts)
            self._logger.info(f"Executing: {docker_cmd}")
            if 0 != os.system(docker_cmd):
                assert False, f"failed : unable to build image {tagged} with buildx"

            push_output = self.push(tagged)
            self._logger.info(push_output)
        else:
            # build and upload image to registry if running remotely
            self._logger.info(f"Building image {tagged} with docker python sdk")
            _, output = self._docker_client.images.build(
                path=image_source_local_path,
                dockerfile=docker_file_path,
                rm=True,
                tag=tagged,
            )
            for o in output:
                self._logger.info(o)
            push_output = self.push(tagged)
            self._logger.info(push_output)