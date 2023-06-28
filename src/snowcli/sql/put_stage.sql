{% include "set_env.sql" %}

{% if create_stage_command %}
{{ create_stage_command }};
{% endif %}

put file://{{ path }} {{ name }}{{ destination_path }} auto_compress=false parallel={{ parallel }} overwrite={{ overwrite }};
