-- package script (2/2)

create or replace table <% ctx.entities.pkg.identifier %>.my_shared_content.shared_table (
  col1 number,
  col2 varchar
);
grant select on table <% ctx.entities.pkg.identifier %>.my_shared_content.shared_table
  to share in application package <% ctx.entities.pkg.identifier %>;
