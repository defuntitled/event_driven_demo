'use strict';
const $ = id => document.getElementById(id);
const roles = ['orders','payments','reservations'];
const labels = {PROCESSING:'В обработке',PAID_PENDING_RESERVATION:'Оплата прошла. Ждём бронирование',CONFIRMED:'Билет подтверждён',REFUND_PENDING:'Возврат в процессе',REFUNDED:'Отменён, деньги возвращены'};
let session, operation, selection='', fence=0, busy=false, pending=null, followed='', chosen=null;
let records=[], eventCursor=0, attempts={}, attemptCursors={}, eventsOK=false, attemptsOK={}, attemptErrors={}, attemptTimes={};
const esc = value => String(value??'—').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const stamp = ms => ms ? new Date(ms).toLocaleTimeString('ru-RU') : 'время неизвестно';
const money = cents => new Intl.NumberFormat('ru-RU',{style:'currency',currency:'RUB',maximumFractionDigits:0}).format(cents/100);
async function api(path, options) {const r=await fetch(path,{...options,signal:AbortSignal.timeout(10000)});const v=await r.json();if(!r.ok)throw new Error(v.error?.message||'Источник недоступен');return v;}
function table(rows, columns){if(!rows.length)return '<p class="muted small">Записей пока нет</p>';return '<div class="table-scroll"><table><thead><tr>'+columns.map(c=>'<th>'+esc(c)+'</th>').join('')+'</tr></thead><tbody>'+rows.map(row=>'<tr>'+columns.map(c=>'<td>'+esc(row[c])+'</td>').join('')+'</tr>').join('')+'</tbody></table></div>';}
function setHTML(id,html){const element=$(id);if(element.dataset.html===html)return;const open=[...element.querySelectorAll('details')].map(d=>d.open);element.innerHTML=html;element.dataset.html=html;element.querySelectorAll('details').forEach((d,i)=>{d.open=open[i]||false;});}
function resetFeeds(){fence++;eventsOK=false;attemptsOK={};attemptErrors={};attemptTimes={};records=[];eventCursor=0;attempts={};attemptCursors={};chosen=null;for(const id of ['events','attempts','money','databases']){$(id).replaceChildren();delete $(id).dataset.html;}for(const id of ['proof-records','proof-attempts','proof-charges','net','seats'])$(id).textContent='—';}
function query(){return '?run_id='+encodeURIComponent(session.run_id)+'&order_id='+encodeURIComponent(selection);}
function controls(){document.querySelectorAll('[data-scenario],#reset').forEach(b=>b.disabled=busy||Boolean(operation));}
async function command(path, body){if(busy)return;busy=true;controls();pending={path,body,key:crypto.randomUUID()};await deliverCommand();}
async function deliverCommand(){try{const result=await api(pending.path,{method:'POST',headers:{'Content-Type':'application/json','Idempotency-Key':pending.key},body:JSON.stringify(pending.body)});operation=result;pending=null;$('control-error').textContent='';}catch(e){$('control-error').textContent=e.message+' Повтор сохраняет ключ операции.';}finally{busy=false;controls();}}
document.querySelectorAll('[data-scenario]').forEach(b=>b.addEventListener('click',()=>command('/api/presenter/prepare',{scenario:b.dataset.scenario})));
$('reset').addEventListener('click',()=>command('/api/presenter/reset',{}));
$('order-select').addEventListener('change',e=>{selection=e.target.value;resetFeeds();});
$('actions').addEventListener('click',e=>{const b=e.target.closest('button');if(!b)return;if(b.dataset.retry){command('/api/operations/'+b.dataset.retry+'/retry',{});return;}if(b.dataset.delivery){busy=true;deliverCommand();return;}command('/api/presenter/actions',{run_id:session.run_id,generation:session.generation,action:b.dataset.action});});
function renderState(state){
 const next=state.session;
 if(session?.run_id!==next.run_id){selection='';followed='';resetFeeds();$('order-cards').replaceChildren();$('order-select').innerHTML='<option value="">Все заказы</option>';}
 session=next;operation=state.operation;controls();
 $('run-id').textContent=session.run_id||'—';$('readiness').textContent={READY:'Готово',PREPARING:'Подготовка',ERROR:'Ошибка подготовки'}[session.state]||session.state;
 $('readiness').className=session.state==='READY'?'good':'waiting';
 $('scenario-name').textContent=document.querySelector('[data-scenario="'+session.scenario+'"]')?.textContent||'';
 $('progress').textContent=operation?operation.stage+' · '+operation.state+' · '+Object.entries(operation.components||{}).map(([k,v])=>k+': '+v).join(' / '):'';
 if(operation?.error)$('control-error').textContent=operation.error;
 $('workers').innerHTML=roles.map(role=>{const c=state.components[role];if(!c?.ok)return '<article class="panel"><h3>'+role+'</h3><p class="error">'+esc(c?.error||'Недоступен')+'</p></article>';return '<article class="panel"><h3>'+role+'</h3>'+Object.values(c.data.workers).map(w=>'<div class="worker"><strong>'+esc(w.worker)+'</strong><span class="'+(w.actual_state==='RUNNING'?'good':'waiting')+'">'+esc(w.desired_state)+' / '+esc(w.actual_state)+'</span><small class="mono">PID '+esc(w.pid)+' · exit '+esc(w.exit_code)+'</small></div>').join('')+'<small class="muted">Замер '+stamp(c.data.sampled_at_ms)+'</small></article>';}).join('');
 let action='';
 if(operation?.state==='FAILED')action='<button data-retry="'+esc(operation.operation_id)+'">Повторить подготовку</button>';
 if(pending)action+='<button data-delivery="true">Повторить управляющий запрос</button>';
 if(!operation&&session.state==='READY'){
  const recovery=[['reservation_paused','reservations','consumer','PAUSED','resume_reservations','Возобновить бронирование'],['payment_crash','payments','consumer','FAULTED','restore_payments','Восстановить платежи'],['orders_outbox_paused','orders','publisher','PAUSED','resume_orders_outbox','Возобновить отправитель']];
  for(const [scenario,role,mode,desired,key,label] of recovery){const w=state.components[role]?.data?.workers?.[mode];if(session.scenario===scenario&&w?.desired_state===desired&&['STOPPED','EXITED','FAULTED'].includes(w.actual_state))action+='<button data-action="'+key+'">'+label+'</button>';}
 }
 $('actions').innerHTML=action;
}
async function orders(){const run=session.run_id;try{let cursor=0,list=[],page;do{page=await api('/api/presenter/orders?run_id='+encodeURIComponent(run)+'&after='+cursor+'&limit=200');if(run!==session.run_id)return;list.push(...page.items);cursor=page.next_cursor;}while(page.has_more);
 let saved;try{saved=JSON.parse(localStorage.getItem('last-ticket-purchase'));}catch{}
 if(saved?.run_id===run&&saved.order_id&&saved.order_id!==followed){followed=saved.order_id;selection=followed;resetFeeds();}
 const select=$('order-select');const options=list.map(o=>o.order_id).join();if(select.dataset.orders!==options){select.innerHTML='<option value="">Все заказы</option>'+list.map(o=>'<option value="'+esc(o.order_id)+'">'+esc(o.buyer_id)+' · '+esc(o.order_id.slice(-8))+'</option>').join('');select.dataset.orders=options;}select.value=selection;
 $('order-cards').innerHTML=list.map(o=>'<article class="order-card panel"><span class="eyebrow">'+esc(o.buyer_id)+'</span><h3 class="'+(['CONFIRMED','REFUNDED'].includes(o.status)?'good':'waiting')+'">'+esc(labels[o.status]||o.status)+'</h3><p class="mono small">'+esc(o.order_id)+'</p><p class="small">'+esc(o.ticket_id||'Билет ещё не выдан')+' · v'+esc(o.version)+'</p></article>').join('')||'<p class="muted">Заказов нет. Покупка начинается на странице покупателя.</p>';$('orders-error').textContent='';}catch(e){$('orders-error').textContent=e.message;}}
function coordinate(r){return r.topic+':'+r.partition_id+':'+r.offset_value;}
function renderFeeds(){
 setHTML('events',records.map(r=>'<article class="record '+(chosen?.id===r.event_id?'selected':'')+'"><button class="record-link" data-event="'+esc(r.event_id)+'" data-coordinate="'+esc(coordinate(r))+'">'+esc(r.event_type)+' <span class="mono small">'+esc(coordinate(r))+'</span></button><p class="mono small muted">'+esc(r.event_id)+'</p><details><summary>Исходное событие</summary><pre>'+esc(r.raw_json)+'</pre></details></article>').join('')||'<p class="muted">Физических записей пока нет</p>');
 const all=roles.flatMap(role=>(attempts[role]||[]).map(r=>({...r,service:role}))).sort((a,b)=>a.started_at_ms-b.started_at_ms);
 setHTML('attempts',all.map(r=>'<article class="record '+(chosen?.coord===coordinate(r)?'selected':chosen?.id===r.event_id?'related':'')+'"><strong>'+esc(r.service)+' · '+esc(r.outcome)+'</strong><p class="mono small">'+esc(coordinate(r))+'</p><p class="small">'+esc(r.commit_status)+' · '+stamp(r.started_at_ms)+'</p><details><summary>Диагностика попытки</summary><pre>'+esc(r.raw_json)+'\n'+esc(r.error_text)+'</pre></details><p class="mono small muted">'+esc(r.worker_instance_id)+'</p></article>').join('')||'<p class="muted">Попыток пока нет</p>');
 $('proof-records').textContent=eventsOK?records.filter(r=>r.event_type==='OrderCreated').length:'—';
 $('proof-attempts').textContent=attemptsOK.payments?(attempts.payments||[]).filter(r=>r.topic==='orders.events').length:'—';
 $('attempts-error').textContent=Object.entries(attemptErrors).map(([role,message])=>role+': '+message).join(' / ');
 $('attempts-time').textContent=roles.map(role=>role+' '+stamp(attemptTimes[role])).join(' · ');
}
$('events').addEventListener('click',e=>{const b=e.target.closest('[data-event]');if(b){chosen={id:b.dataset.event,coord:b.dataset.coordinate};renderFeeds();}});
async function feeds(){const mine=fence,q=query();await Promise.all([ (async()=>{try{const p=await api('/api/presenter/events'+q+'&after='+eventCursor+'&limit=200');if(mine!==fence)return;records.push(...p.items);eventCursor=p.next_cursor;eventsOK=!p.has_more;$('events-time').textContent='Замер '+stamp(p.sampled_at_ms);renderFeeds();$('events-error').textContent='';}catch(e){if(mine===fence){eventsOK=false;renderFeeds();$('events-error').textContent=e.message;}}})(),...roles.map(async role=>{try{const p=await api('/api/presenter/attempts'+q+'&service='+role+'&after='+(attemptCursors[role]||0)+'&limit=200');if(mine!==fence)return;attempts[role]??=[];attempts[role].push(...p.items);attemptCursors[role]=p.next_cursor;/* Refresh mutable commit state of recent attempts on every sample. */const recent=await api('/api/presenter/attempts'+q+'&service='+role+'&after='+Math.max(0,(attemptCursors[role]||0)-200)+'&limit=200');if(mine!==fence)return;const byID=new Map(attempts[role].map(a=>[a._cursor,a]));recent.items.forEach(a=>byID.set(a._cursor,a));attempts[role]=[...byID.values()];attemptsOK[role]=!p.has_more;attemptTimes[role]=recent.sampled_at_ms;delete attemptErrors[role];renderFeeds();}catch(e){if(mine===fence){attemptsOK[role]=false;attemptErrors[role]=e.message;renderFeeds();}}})]);}
async function databases(){const mine=fence;try{const value=await api('/api/presenter/databases'+query());if(mine!==fence)return;const cs=value.components,p=cs.payments,rs=cs.reservations;
 $('db-error').textContent=Object.entries(cs).filter(([,c])=>!c.ok).map(([r,c])=>r+': '+c.error.message).join(' / ');
 if(p.ok){const ops=p.data.payment_operations;$('proof-charges').textContent=ops.filter(o=>o.kind==='CHARGE').length;$('net').textContent=money(ops.reduce((s,o)=>s+(o.kind==='CHARGE'?1:-1)*o.amount_cents,0));$('money').innerHTML=table(ops,['kind','amount_cents','currency','order_id','payment_id','original_payment_id']);}else{for(const id of ['proof-charges','net'])$(id).textContent='—';$('money').textContent='Денежный журнал недоступен';}
 $('seats').textContent=rs.ok?rs.data.inventory[0]?.available??'—':'—';$('db-time').textContent=roles.map(r=>r+' '+stamp(cs[r].sampled_at_ms)).join(' · ');
 setHTML('databases',roles.map(role=>'<h3>'+role+'</h3>'+(cs[role].ok?Object.entries(cs[role].data).map(([name,rows])=>'<details><summary>'+name+' · '+rows.length+'</summary>'+table(rows,rows.length?Object.keys(rows[0]).filter(k=>k!=='_cursor'):[])+'</details>').join(''):'<p class="error">Источник недоступен</p>')).join(''));
 }catch(e){if(mine===fence)$('db-error').textContent=e.message;}}
async function poll(){try{renderState(await api('/api/presenter/state'));if(session.run_id){await orders();await Promise.all([feeds(),databases()]);}$('connection-error').textContent='';}catch(e){$('connection-error').textContent=e.message;}finally{setTimeout(poll,500);}}
async function kafka(){try{const k=await api('/api/presenter/kafka');$('kafka-time').textContent='Замер '+stamp(k.sampled_at_ms);$('kafka-error').textContent=k.ok?'':k.error||'Kafka недоступна';$('kafka').innerHTML=k.ok?table(k.items,['group_id','topic','partition','committed_next_offset','log_end_offset','lag']):'<p class="muted">Нет актуальных позиций</p>';}catch(e){$('kafka-error').textContent=e.message;}finally{setTimeout(kafka,1000);}}
poll();kafka();
