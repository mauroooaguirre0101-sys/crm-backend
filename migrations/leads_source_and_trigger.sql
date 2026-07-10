-- Atribución de leads: lead_creation_source + inmutabilidad de first_touch
--
-- Ejecutar DESPUÉS de leads_attribution.sql (que agrega attr_first_touch / attr_last_touch).
--
-- lead_creation_source: cómo fue creado el lead en el CRM.
--   'manual'      → creado por el equipo desde el frontend
--   'automation'  → creado automáticamente por un ContactCreate de GHL
--   'appointment' → creado automáticamente cuando llegó un appointment sin lead previo
--   'import'      → importado en batch
--   'api'         → creado vía API externa

ALTER TABLE leads
  ADD COLUMN IF NOT EXISTS lead_creation_source TEXT DEFAULT 'manual';

-- Trigger: attr_first_touch es completamente inmutable una vez establecido.
-- Solo permite la transición NULL → valor.
-- Cualquier intento de modificar o limpiar un first_touch ya establecido
-- lanza una excepción, incluso desde migraciones o queries manuales.

CREATE OR REPLACE FUNCTION leads_protect_first_touch()
RETURNS TRIGGER AS $$
BEGIN
  IF OLD.attr_first_touch IS NOT NULL
     AND NEW.attr_first_touch IS DISTINCT FROM OLD.attr_first_touch
  THEN
    RAISE EXCEPTION
      'attr_first_touch is immutable once set (leads.id = %)', OLD.id
      USING ERRCODE = 'raise_exception';
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_leads_protect_first_touch ON leads;
CREATE TRIGGER trg_leads_protect_first_touch
  BEFORE UPDATE ON leads
  FOR EACH ROW
  EXECUTE FUNCTION leads_protect_first_touch();
