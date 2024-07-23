-- app post-deploy script (1/2)

CREATE SCHEMA IF NOT EXISTS &{ ctx.env.schema };
CREATE TABLE IF NOT EXISTS &{ ctx.env.schema }.post_deploy_log (text VARCHAR);
INSERT INTO &{ ctx.env.schema }.post_deploy_log VALUES('post-deploy-part-1');
