'use strict';
const $ = id => document.getElementById(id);
const labels = {PROCESSING:'В обработке',PAID_PENDING_RESERVATION:'Оплата прошла. Ждём бронирование',CONFIRMED:'Билет подтверждён',REFUND_PENDING:'Возврат в процессе',REFUNDED:'Отменён, деньги возвращены'};
let session = null, saved = null, sending = false, version = 0, epoch = 0, storageOK = true, retryAt = 0, failures = 0, fatal = false;
try { saved = JSON.parse(localStorage.getItem('last-ticket-purchase')); } catch { saved = null; }
function persist() { try { localStorage.setItem('last-ticket-purchase',JSON.stringify(saved)); storageOK=true; return true; } catch { storageOK=false; $('error').textContent='Хранилище браузера недоступно: невозможно сохранить ключ покупки. Разрешите сохранение данных и обновите страницу.'; return false; } }
async function api(path,options) {const response=await fetch(path,{...options,signal:AbortSignal.timeout(8000)});const value=await response.json();if(!response.ok){const error=new Error(value.error?.message || 'Обновление временно недоступно');error.code=value.error?.code;error.retryable=value.error?.retryable??response.status>=500;throw error;}return value;}
function render() {$('buy').disabled=!session?.can_purchase || !storageOK || sending || Boolean(saved?.order_id) || Boolean(saved?.key);}
async function send() {
 if (sending || !storageOK || fatal || Date.now()<retryAt || !saved?.key || saved.order_id) return;
 sending=true;const mine=epoch;const request={...saved};render();
 try {const result=await api('/api/orders',{method:'POST',headers:{'Content-Type':'application/json','Idempotency-Key':request.key},body:JSON.stringify(request.body)});if(mine!==epoch||result.run_id!==session?.run_id)return;saved.order_id=result.order_id;persist();$('order-id').textContent=result.order_id;$('error').textContent='';}
 catch(error){if(mine===epoch){failures++;retryAt=Date.now()+Math.min(2000,250*2**Math.min(failures,3));fatal=error.retryable===false;if(fatal){saved.failed=error.message;persist();}$('error').textContent=error.message;}}
 finally {sending=false;render();}
}
$('buy').addEventListener('click',async()=>{if(!session?.can_purchase||saved?.key)return;saved={run_id:session.run_id,key:crypto.randomUUID(),body:{run_id:session.run_id,concert_id:'concert-1',quantity:1}};if(!persist())return;$('status').textContent='В обработке';await send();});
async function poll(){
 try {
  const next=await api('/api/session');
  if(session?.run_id!==next.run_id){epoch++;version=0;fatal=Boolean(saved?.failed);retryAt=0;failures=0;if(saved?.run_id!==next.run_id){saved=null;fatal=false;persist();$('order-id').textContent='';$('ticket-id').textContent='';}$('status').textContent=next.state==='READY'?'Готово к покупке':'Подготовка';}
  session=next;const mine=epoch;render();
  if(!session.run_id)return;
  const concert=await api('/api/concert?run_id='+encodeURIComponent(session.run_id));if(mine!==epoch)return;
  $('available').textContent=concert.available===null?'Наличие временно недоступно':'Свободных мест: '+concert.available;
  $('sampled').textContent=concert.sampled_at_ms?'Проверено '+new Date(concert.sampled_at_ms).toLocaleTimeString('ru-RU'):'';
  if(saved?.key&&!saved.order_id)await send();
  if(saved?.order_id){const order=await api('/api/orders/'+saved.order_id+'?run_id='+session.run_id);if(mine!==epoch||order.run_id!==session.run_id||order.version<version)return;version=order.version;$('status').textContent=labels[order.status]||order.status;$('status').className=['CONFIRMED','REFUNDED'].includes(order.status)?'good':'waiting';$('order-id').textContent=order.order_id;$('ticket-id').textContent=order.ticket_id?'Билет: '+order.ticket_id:'';}
  if(storageOK&&!fatal&&(!saved?.key||saved.order_id))$('error').textContent='';
  if(saved?.failed)$('error').textContent=saved.failed;
 }catch(error){if(storageOK)$('error').textContent='Обновление временно недоступно. '+error.message;}
 finally{render();setTimeout(poll,500);}
}
poll();
