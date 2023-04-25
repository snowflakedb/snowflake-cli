use role {role};
use warehouse {warehouse};
use database {database};
use schema {schema};

set service_properties = $$
  {{"compute_pool": "{compute_pool}",
   "min_instances": {num_instances},
   "max_instances": {num_instances},
   "container": [
    {{
      "image": "{image}"
    }}
   ]
  }}
$$;

-- Create the service. This will also start running the service if creation is successful.
call system$create_snowservice('{name}', $service_properties);
