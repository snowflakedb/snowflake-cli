-- package post-deploy script (1/2)

select &{ ctx.native_app.name };
select &{ ctx.env.package_foo };
