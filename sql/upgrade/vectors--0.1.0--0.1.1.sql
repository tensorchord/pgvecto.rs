\echo Use "ALTER EXTENSION vectors UPDATE TO '0.2.0'" to load this file. \quit

DO LANGUAGE plpgsql $$
    DECLARE
    BEGIN
        IF '@extschema@' != 'vectors' THEN
            RAISE EXCEPTION 'Please read upgrade document in https://docs.pgvecto.rs/admin/upgrading.html#upgrade-from-0-1-x.';
        END IF;
    END;
$$;

CREATE FUNCTION "cast_vector_to_array"(
    "vector" vector,
    "_typmod" INT,
    "_explicit" bool
) RETURNS real[]
STRICT
LANGUAGE c AS 'MODULE_PATHNAME', 'cast_vector_to_array_wrapper';

CREATE FUNCTION "cast_array_to_vector"(
    "array" real[],
    "typmod" INT,
    "_explicit" bool
) RETURNS vector
STRICT
LANGUAGE c AS 'MODULE_PATHNAME', 'cast_array_to_vector_wrapper';

CREATE CAST (real[] AS vector)
    WITH FUNCTION cast_array_to_vector(real[], integer, boolean) AS IMPLICIT;

CREATE CAST (vector AS real[])
    WITH FUNCTION cast_vector_to_array(vector, integer, boolean) AS IMPLICIT;
