-- package script (2/2)

create or replace table &{ ctx.entities.pkg.name }.my_shared_content.shared_table (
  col1 number,
  col2 varchar
);

insert into &{ ctx.entities.pkg.name }.my_shared_content.shared_table (col1, col2)
  values (1, 'hello');

grant select on table &{ ctx.entities.pkg.name }.my_shared_content.shared_table
  to share in application package &{ ctx.entities.pkg.name };
