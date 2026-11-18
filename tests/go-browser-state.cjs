const {chromium}=require(process.env.PLAYWRIGHT_MODULE||'/tmp/last-ticket-browser/node_modules/playwright');
const assert=require('node:assert/strict');
const base=process.env.DEMO_BASE_URL||'http://localhost:18080';
(async()=>{
 const browser=await chromium.launch({headless:true,executablePath:process.env.CHROME_PATH||'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'});
 try{
 const context=await browser.newContext(),page=await context.newPage();
 const prepare=async()=>{const response=await context.request.post(base+'/api/presenter/prepare',{headers:{'Idempotency-Key':crypto.randomUUID()},data:{scenario:'happy_path'}});assert.equal(response.status(),202);const op=await response.json();for(let i=0;i<300;i++){const v=await(await context.request.get(base+'/api/operations/'+op.operation_id)).json();if(v.state==='SUCCEEDED')return op.run_id;await new Promise(r=>setTimeout(r,100));}throw Error('prepare timeout');};
 const run=await prepare();let lost=false;const keys=[];
 await page.route('**/api/orders',async route=>{keys.push(route.request().headers()['idempotency-key']);if(!lost){lost=true;await route.fetch();await route.abort('failed');return;}await route.continue();});
 await page.goto(base);await page.getByRole('button',{name:'Купить за 500 ₽',exact:true}).click();
 await page.waitForFunction(()=>document.getElementById('error').textContent.length>0);
 const before=await page.evaluate(()=>JSON.parse(localStorage.getItem('last-ticket-purchase')));
 await page.reload();await page.getByText('Билет подтверждён',{exact:true}).waitFor({timeout:45000});
 const after=await page.evaluate(()=>JSON.parse(localStorage.getItem('last-ticket-purchase')));
 assert.equal(after.key,before.key);assert(keys.length>=2);assert(keys.every(k=>k===before.key));
 const orders=await(await context.request.get(base+'/api/presenter/orders?run_id='+run)).json();assert.equal(orders.items.length,1);
 // A delayed old version must not regress a terminal status.
 await page.route('**/api/orders/*?**',route=>route.fulfill({contentType:'application/json',body:JSON.stringify({run_id:run,order_id:after.order_id,version:0,status:'PROCESSING'})}));
 await page.waitForTimeout(1000);assert.equal(await page.locator('#status').textContent(),'Билет подтверждён');
 await page.unroute('**/api/orders/*?**');await page.unroute('**/api/orders');
 // Hold an accepted POST across a reset and then release its stale response.
 await prepare();let release,arrived;const delayed=new Promise(resolve=>release=resolve),accepted=new Promise(resolve=>arrived=resolve);
 await page.route('**/api/orders',async route=>{const response=await route.fetch();arrived();await delayed;await route.fulfill({response});});
 await page.getByRole('button',{name:'Купить за 500 ₽',exact:true}).click();await accepted;
 const newRun=await prepare();await page.waitForFunction(()=>localStorage.getItem('last-ticket-purchase')==='null');release();
 await page.waitForTimeout(500);assert.equal(await page.locator('#order-id').textContent(),'');
 await page.unroute('**/api/orders');await page.getByRole('button',{name:'Купить за 500 ₽',exact:true}).click();await page.getByText('Билет подтверждён',{exact:true}).waitFor({timeout:45000});
 assert.equal(await page.evaluate(()=>JSON.parse(localStorage.getItem('last-ticket-purchase')).run_id),newRun);
 await prepare();const broken=await context.newPage();await broken.addInitScript(()=>{Storage.prototype.setItem=function(){throw new DOMException('quota','QuotaExceededError')}});
 let posts=0;broken.on('request',r=>{if(r.method()==='POST'&&r.url().endsWith('/api/orders'))posts++});
 await broken.goto(base);await broken.waitForTimeout(2000);
 assert.match(await broken.locator('#error').textContent(),/сохран|хранилищ/i);assert.equal(posts,0);
 const presenter=await context.newPage();
 let ready;const sampled=new Promise(resolve=>ready=resolve);
 await presenter.route('**/api/presenter/attempts?**',async route=>{
  const role=new URL(route.request().url()).searchParams.get('service');
  if(role==='payments'){await route.fulfill({status:503,contentType:'application/json',body:JSON.stringify({error:{message:'payments test unavailable'}})});return;}
  await new Promise(r=>setTimeout(r,150));await route.fulfill({contentType:'application/json',body:JSON.stringify({items:[],next_cursor:0,has_more:false,sampled_at_ms:Date.now()})});
  if(role==='reservations')ready();
 });
 await presenter.goto(base+'/presenter');await sampled;await presenter.waitForTimeout(200);
 assert.match(await presenter.locator('#attempts-error').textContent(),/payments test unavailable/);
 assert.equal(await presenter.locator('#proof-attempts').textContent(),'—');
 console.log(JSON.stringify({lost_response:'same key, one order',storage_failure:'purchase blocked',old_version:'ignored',stale_post:'ignored after reset',partial_source_error:'payments error remains visible'}));
 }finally{await browser.close()}
})().catch(e=>{console.error(e);process.exitCode=1});
