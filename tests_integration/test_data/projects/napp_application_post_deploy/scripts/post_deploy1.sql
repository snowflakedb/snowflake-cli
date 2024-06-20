-- app post-deploy script (1/2)

CREATE SCHEMA IF NOT EXISTS public;
CREATE TABLE IF NOT EXISTS public.post_deploy_log (text VARCHAR);
INSERT INTO public.post_deploy_log VALUES('post-deploy-part-1');
