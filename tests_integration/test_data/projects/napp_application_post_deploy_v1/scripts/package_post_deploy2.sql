-- app post-deploy script (2/2)

INSERT INTO &{ ctx.env.pkg_schema }.post_deploy_log VALUES('package-post-deploy-part-2');
-- comment at the end of the file to make sure no Empty SQL statement error
