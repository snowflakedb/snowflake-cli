-- app post-deploy script (1/2)

CREATE SCHEMA IF NOT EXISTS &{ ctx.env.pkg_schema };
CREATE TABLE IF NOT EXISTS &{ ctx.env.pkg_schema }.post_deploy_log (text VARCHAR);
INSERT INTO &{ ctx.env.pkg_schema }.post_deploy_log VALUES('package-post-deploy-part-1');
