-- ============================================================
-- ghl_calificacion_template: define las preguntas canónicas
-- que se deben guardar cuando llega un webhook de GHL.
-- Filtra duplicados causados por custom fields viejos en GHL.
--
-- Antes de ejecutar: reemplazá 'TU_CLIENTE_ID' con el ID real
-- del cliente. Lo encontrás en la tabla ghl_connections o
-- user_clientes de Supabase.
-- ============================================================

INSERT INTO form_templates (cliente_id, tipo, questions, updated_at)
VALUES (
  'TU_CLIENTE_ID',
  'ghl_calificacion',
  '[
    {"id":"q1","titulo":"Como es tu nombre de instagram (por ej @gabifleita2032)"},
    {"id":"q2","titulo":"¿Qué es lo que más te llama la atención de la idea de Importar de China?"},
    {"id":"q3","titulo":"¿Qué te gustaría lograr importando de China?"},
    {"id":"q4","titulo":"¿Qué nivel de ingresos te gustaría alcanzar importando de China?"},
    {"id":"q5","titulo":"Luego de ayudar a más de 100 alumnos a tener éxito en el negocio de la Importación, me di cuenta que se necesita un capital mínimo de aproximadamente $3000 para todo el proceso. ¿Actualmente cuanto capital tenes para invertir en este proyecto?"},
    {"id":"q6","titulo":"En caso de que tengas que tomar una decisión en la llamada, ¿hace falta que alguien más este presente?"}
  ]'::jsonb,
  NOW()
)
ON CONFLICT (cliente_id, tipo)
DO UPDATE SET questions = EXCLUDED.questions, updated_at = NOW();
