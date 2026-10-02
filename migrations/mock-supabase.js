(function(){
  var today = new Date().toISOString().slice(0,10);
  var TABLES = {
    campaigns: [{ id:'c1', nombre:'CBO Testeo Musculosa', tipo_id:null, producto_id:null, plataforma:'Meta', fecha_inicio:today, estado:'activo' }],
    tests: [
      { id:'t1', nombre:'AD 1', campaign_id:'c1', fecha_inicio:today, estado:'activo', spend:0, revenue:0 },
      { id:'t2', nombre:'AD 2', campaign_id:'c1', fecha_inicio:today, estado:'activo', spend:0, revenue:0 },
      { id:'t3', nombre:'AD 3', campaign_id:'c1', fecha_inicio:today, estado:'activo', spend:0, revenue:0 },
    ],
  };
  function thenable(result){ return { then: function(resolve){ return Promise.resolve().then(function(){ return resolve(result); }); } }; }
  function makeQuery(t){
    var rows = TABLES[t] || [];
    var q = {
      select: function(){ return q; }, order: function(){ return q; }, eq: function(){ return q; },
      single: function(){ return thenable({ data:null, error:null }); },
      insert: function(){ return thenable({error:null}); },
      update: function(){ return { eq: function(){ return thenable({error:null}); } }; },
      delete: function(){ return { eq: function(){ return thenable({error:null}); } }; },
      then: function(resolve){ return Promise.resolve().then(function(){ return { data: rows.slice(), error: null }; }).then(resolve); },
    };
    return q;
  }
  window.supabase = { createClient: function(){ return { from: function(t){ return makeQuery(t); }, functions: { invoke: function(){ return Promise.resolve({data:{ok:true},error:null}); } } }; } };
})();
