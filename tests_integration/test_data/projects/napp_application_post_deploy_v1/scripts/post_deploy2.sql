-- app post-deploy script (2/2)

INSERT INTO &{ ctx.env.schema }.post_deploy_log VALUES('post-deploy-part-2');
