const { chromium } = require(process.env.PLAYWRIGHT_MODULE || '/tmp/last-ticket-browser/node_modules/playwright');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const base = process.env.DEMO_BASE_URL || 'http://localhost:18080';
(async () => {
 const browser=await chromium.launch({headless:true,executablePath:process.env.CHROME_PATH||'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'});
 try {
  const context=await browser.newContext({viewport:{width:1440,height:1050}}), buyer=await context.newPage(), presenter=await context.newPage();
  const errors=[];for(const page of [buyer,presenter])page.on('pageerror',e=>errors.push(e.message));
  await buyer.goto(base);await presenter.goto(base+'/presenter');
  await presenter.getByRole('heading',{name:'За кулисами покупки',exact:true}).waitFor({timeout:3000});
  const timings=[];
  for(const [scenario,label,action] of [
   ['happy_path','Обычная покупка',null],
   ['reservation_paused','Приостановить бронирование','Возобновить бронирование'],
   ['race_last_ticket','Два покупателя, одно место',null],
   ['payment_crash','Сбой платежей сразу после списания','Восстановить платежи'],
   ['orders_outbox_paused','Приостановить отправитель событий','Возобновить отправитель']]) {
   const started=Date.now(), previous=await presenter.locator('#run-id').textContent();
   await presenter.getByRole('button',{name:label,exact:true}).click();
   await presenter.waitForFunction(old=>document.getElementById('run-id').textContent!==old,previous,{timeout:45000});
   await presenter.getByText('Готово',{exact:true}).waitFor({timeout:45000});
   const buy=buyer.getByRole('button',{name:'Купить за 500 ₽',exact:true});await buy.click({timeout:45000});
   if(scenario==='reservation_paused')await buyer.getByText('Оплата прошла. Ждём бронирование',{exact:true}).waitFor({timeout:15000});
   if(action)await presenter.getByRole('button',{name:action,exact:true}).click({timeout:45000});
   await buyer.waitForFunction(()=>['Билет подтверждён','Отменён, деньги возвращены'].includes(document.getElementById('status').textContent),null,{timeout:45000});
   if(scenario==='race_last_ticket')await presenter.waitForFunction(()=>document.querySelectorAll('.order-card').length===2&&document.querySelector('#order-cards').textContent.includes('деньги возвращены'),null,{timeout:45000});
   if(scenario==='payment_crash')await presenter.waitForFunction(()=>document.getElementById('proof-attempts').textContent==='2',null,{timeout:45000});
   const runID=await presenter.locator('#run-id').textContent(),orderID=await buyer.locator('#order-id').textContent();
   const snapshot={scenario,run_id:runID,order_id:orderID};
   for(const source of ['databases','events','kafka'])snapshot[source]=await(await context.request.get(base+'/api/presenter/'+source+'?run_id='+runID+'&order_id='+orderID)).json();
   fs.writeFileSync('artifacts/go-'+scenario+'.json',JSON.stringify(snapshot,null,2));
   await presenter.screenshot({path:'artifacts/go-'+scenario+'.png',fullPage:true});
   timings.push({scenario,duration_ms:Date.now()-started,run_id:await presenter.locator('#run-id').textContent(),order_id:await buyer.locator('#order-id').textContent()});
  }
  for(const width of [375,768,1024,1440]){for(const page of [buyer,presenter]){await page.setViewportSize({width,height:1000});assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true,'horizontal page overflow at '+width);}}
  assert.deepEqual(errors,[]);
  fs.writeFileSync('artifacts/go-browser.json',JSON.stringify({base,completed_at:new Date().toISOString(),timings,errors},null,2));console.log(JSON.stringify({timings,errors}));
 } finally {await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
