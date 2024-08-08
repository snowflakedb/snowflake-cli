-- app post-deploy script (2/2)

INSERT INTO &{ ctx.env.pkg_schema }.post_deploy_log VALUES('package-post-deploy-part-2');
