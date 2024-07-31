-- package script (1/2)

create schema if not exists {{ package_name }}.my_shared_content;
grant usage on schema {{ package_name }}.my_shared_content
  to share in application package {{ package_name }};
