const { chromium } = require('playwright');
(async()=>{
  const b=await chromium.launch();
  const p=await b.newPage({userAgent:'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'});
  const hits=[];
  p.on('request',r=>{ if(/facebook\.com\/tr/.test(r.url())) hits.push(r.url()); });
  await p.goto('https://easylifeuy.myshopify.com/products/dispensador-de-pasta-dental-esterilizador-de-cepillos',{waitUntil:'load'});
  await p.waitForTimeout(3000);
  console.log('window.fbq existe:', await p.evaluate(()=>typeof window.fbq));
  console.log('llamadas a facebook.com/tr:', hits.length);
  hits.forEach(h=>{ const u=new URL(h); console.log(' ev=',u.searchParams.get('ev'),'| id=',u.searchParams.get('id')); });
  await b.close();
})();
