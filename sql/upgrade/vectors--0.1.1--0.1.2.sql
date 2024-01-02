\echo Use "ALTER EXTENSION vectors UPDATE TO '0.1.2'" to load this file. \quit

DO LANGUAGE plpgsql $$
    DECLARE
    BEGIN
        RAISE EXCEPTION 'Upgrade from version 0.1.1 is not supported.';
    END;
$$;
