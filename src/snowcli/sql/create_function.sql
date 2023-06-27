{% include "set_env.sql" %}
CREATE {% if overwrite %}OR REPLACE {% endif %} FUNCTION {{ name }}{{ input_parameters }}
         RETURNS {{ return_type }}
         LANGUAGE PYTHON
         RUNTIME_VERSION=3.8
         IMPORTS=('{{ imports }}')
         HANDLER='{{ handler }}'
         PACKAGES=({% for pkg in packages %}'{{ pkg }}'{{ "," if not loop.last }}{% endfor %});
describe function {{ name }}{{ signature }};
