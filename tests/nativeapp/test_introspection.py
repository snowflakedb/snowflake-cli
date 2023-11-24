from snowcli.cli.nativeapp.introspection import extract_execute_immediate_relpaths


def test_extract_execute_immediate_relpaths():
    assert (
        extract_execute_immediate_relpaths(
            r"""
            -- basic example
            execute immediate from './setup-part-2.sql';

            -- weird spacing + case
            EXECUTE
                        immediate
                        
                        from
'../../../bababa.sql'
;
            -- proper escaping of quotes
            execute immediate from './\'quoted file\'.sql';

            -- newline characters can't be inside a string
            execute immediate from '
            ./abc.sql';

            -- other escape characters
            execute immediate from './\n_\u26c4_char.sql';
            execute immediate from './back\\slash.sql';

            -- don't pick up execute immediate statements in comments
            -- execute immediate from './comment1.sql';
            /* execute immediate from './comment2.sql'; */
            /*
                execute immediate
                from
                './comment3.sql';
            */            
            """
        )
        == [
            "./setup-part-2.sql",
            "../../../bababa.sql",
            "./'quoted file'.sql",
            "./\n_⛄_char.sql",
            "./back\\slash.sql",
        ]
    )
