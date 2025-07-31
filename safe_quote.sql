CREATE OR REPLACE FUNCTION SAFE_QUOTE(s VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Takes an identifier and returns a version that is safely double-quoted, handling cases where the identifier is already quoted or contains quotes.'
AS
$$
DECLARE
  raw_ident VARCHAR;
BEGIN
  IF (STARTSWITH(s, '"') AND ENDSWITH(s, '"')) THEN
    raw_ident := REPLACE(SUBSTRING(s, 2, LENGTH(s) - 2), '""', '"');
  ELSE
    raw_ident := s;
  END IF;
  RETURN '"' || REPLACE(raw_ident, '"', '""') || '"';
END;
$$;
