How to create a function in Snowflake CLI
================================================================================

Before you create a function, ensure that you have configured a connection in your :file:`config.toml` file as described in :doc:`../connecting/connect`.
The connection should include the database and schema information that is used to create the function.

To create a function in Snowflake CLI:

#. Create and navigate to an empty directory for your function:

   .. code-block:: bash

    $ mkdir ~/my-snowflake-app
    $ cd ~/my-snowflake-app

#. Run the :code:`init` command:

   .. code-block:: bash

    $ snow snowpark function init
    Done

   Snowflake CLI populates the directory with the files for a basic function, similar to the following:

   .. code-block:: output

    __pycache__
    app.py
    config.toml
    requirements.txt

   The :file:`app.py` file includes sample code for the :code:`hello-world` function. The :file:`config.toml` file created in this directory
   is just a template for your configuration. You can modify the file and then use it with the :code:`snow --config-file` flag,
   or you can rely on the global Snowflake CLI configuration.

#. Open the :file:`app.py` file, and make any desired changes:

   .. code-block:: python

    import sys

    def hello() -> str:
        return 'Hello World!'

    # For local debugging. Be aware you may need to type-convert arguments if you add input parameters
    if __name__ == '__main__':
        if len(sys.argv) > 1:
            print(hello(sys.argv[1:]))  # type: ignore
        else:
            print(hello()) # type: ignore

#. To test the code, run the :file:`app.py` script:

   .. code-block:: bash

    $ python app.py

   Sample output:

   .. code-block:: output

    Hello World!

#. To create a ZIP file that contains the necessary files, package the function:

   .. code-block:: bash

    $ snow snowpark function package -v
    Done

   The command creates a ZIP file that matches the app name (:file:`app.zip`, in this case):

   .. code-block:: output

    2023-07-27 09:54:56 INFO Resolving any requirements from requirements.txt...
    2023-07-27 09:54:56 INFO Deployment package now ready: app.zip
