// Reytech RFQ — Shared header JS

// ── Keyboard shortcuts ──
(function(){
  var shortcuts = {'n':'/pricechecks','q':'/quotes','o':'/orders','p':'/pipeline','g':'/growth','b':'/brief','c':'/contacts','i':'/intelligence','v':'/vendors','d':'/debug','h':'/'};
  document.addEventListener('keydown', function(e) {
    var tag = (e.target.tagName||'').toLowerCase();
    if (tag==='input'||tag==='textarea'||tag==='select'||e.target.isContentEditable) return;
    if (e.ctrlKey||e.altKey||e.metaKey) return;
    var key = e.key.toLowerCase();
    if (key==='/'||key==='s') { var s=document.querySelector('input[name="q"],input[type="search"],#search-input'); if(s){e.preventDefault();s.focus();s.select();return;} e.preventDefault();window.location.href='/search';return; }
    if (key==='?') { e.preventDefault(); var o=document.getElementById('kb-help'); if(o){o.remove();return;} o=document.createElement('div');o.id='kb-help';o.style.cssText='position:fixed;inset:0;background:rgba(0,0,0,.7);z-index:9999;display:flex;align-items:center;justify-content:center';o.onclick=function(){o.remove()};o.innerHTML='<div style="background:#1a1b2e;border:1px solid #333;border-radius:12px;padding:24px 32px;max-width:400px;color:#e2e8f0" onclick="event.stopPropagation()"><h3 style="margin:0 0 16px;font-size:18px">Keyboard Shortcuts</h3><div style="display:grid;grid-template-columns:40px 1fr;gap:6px 12px;font-size:14px"><kbd style="background:#333;padding:2px 8px;border-radius:4px;text-align:center;font-family:monospace">/ s</kbd><span>Search</span><kbd style="background:#333;padding:2px 8px;border-radius:4px;text-align:center;font-family:monospace">h</kbd><span>Home</span><kbd style="background:#333;padding:2px 8px;border-radius:4px;text-align:center;font-family:monospace">n</kbd><span>Price Checks</span><kbd style="background:#333;padding:2px 8px;border-radius:4px;text-align:center;font-family:monospace">q</kbd><span>Quotes</span><kbd style="background:#333;padding:2px 8px;border-radius:4px;text-align:center;font-family:monospace">o</kbd><span>Orders</span><kbd style="background:#333;padding:2px 8px;border-radius:4px;text-align:center;font-family:monospace">p</kbd><span>Pipeline</span></div><p style="margin:16px 0 0;font-size:12px;color:#888">Press any key or click outside to close</p></div>';document.body.appendChild(o);return; }
    if (shortcuts[key]) { e.preventDefault(); window.location.href=shortcuts[key]; }
  });
})();

// ── Poll time display ──
function _updatePollTime(ts){
 var el=document.getElementById('poll-time');
 if(el&&ts){el.dataset.utc=ts;try{var d=new Date(ts);if(!isNaN(d)){el.textContent=d.toLocaleString(undefined,{month:'short',day:'numeric',hour:'numeric',minute:'2-digit',hour12:true})}}catch(e){el.textContent=ts}}
}

// ══════════════════════════════════════════════════════════════════════════
// CHECK NOW: Quick poll for NEW unread emails only.
// Already-processed emails are skipped. Fast. Use when expecting new mail.
// ══════════════════════════════════════════════════════════════════════════
function pollNow(btn){
 btn.disabled=true;btn.setAttribute('aria-busy','true');
 btn.textContent='Checking\u2026';
 fetch('/api/poll-now',{credentials:'same-origin'}).then(function(r){return r.json()}).then(function(d){
  _updatePollTime(d.last_check);
  if(d.found>0){
   btn.textContent='\u2705 '+d.found+' new!';
   btn.style.background='rgba(52,211,153,.2)';btn.style.borderColor='rgba(52,211,153,.4)';
   setTimeout(function(){location.reload()},1000);
  } else {
   btn.textContent='\u2705 Up to date';
   btn.style.background='rgba(52,211,153,.1)';
   setTimeout(function(){_resetBtn(btn,'\u26A1 Check Now')},2000);
  }
 }).catch(function(){
  btn.textContent='\u274C Error';btn.style.background='rgba(248,81,73,.15)';
  setTimeout(function(){_resetBtn(btn,'\u26A1 Check Now')},3000);
 });
}

// ══════════════════════════════════════════════════════════════════════════
// RESYNC: Nuclear option. Clears RFQ queue, resets processed-email list,
// re-imports everything from scratch. PCs are preserved.
// Use after a bug fix, missed emails, or when the queue looks stale.
// ══════════════════════════════════════════════════════════════════════════
function resyncAll(btn){
 if(!confirm('RESYNC will:\n\n\u2022 Keep sent/won/lost/draft RFQs (your work is safe)\n\u2022 Clear only new/stale imports\n\u2022 Re-import missed emails from inbox\n\u2022 Price Checks are KEPT\n\nUse after a fix or when emails were missed.\nContinue?'))return;
 btn.disabled=true;btn.setAttribute('aria-busy','true');
 // Build progress overlay
 var bar=document.createElement('div');
 bar.id='resync-progress';
 bar.innerHTML='<div style="position:fixed;top:0;left:0;right:0;z-index:99999;background:#1e293b;padding:10px 20px;display:flex;align-items:center;gap:12px;border-bottom:2px solid #f59e0b;box-shadow:0 2px 12px rgba(0,0,0,.5)">'
  +'<div style="flex:1"><div style="background:#334155;border-radius:4px;height:8px;overflow:hidden">'
  +'<div id="resync-bar" style="height:100%;background:linear-gradient(90deg,#f59e0b,#fbbf24);border-radius:4px;width:5%;transition:width .3s"></div></div></div>'
  +'<span id="resync-status" style="color:#fbbf24;font-size:13px;white-space:nowrap">\uD83D\uDD04 Connecting...</span>'
  +'<span id="resync-timer" style="color:#94a3b8;font-size:12px;font-family:monospace">0s</span></div>';
 document.body.appendChild(bar);
 btn.textContent='\uD83D\uDD04 Resyncing\u2026';
 btn.style.background='rgba(251,191,36,.2)';btn.style.borderColor='rgba(251,191,36,.4)';
 var started=Date.now(),pct=5;
 var steps=[
  {at:500,pct:10,msg:'\uD83D\uDD04 Clearing stale imports...'},
  {at:2000,pct:20,msg:'\uD83D\uDCE7 Connecting to inbox...'},
  {at:5000,pct:35,msg:'\uD83D\uDCE7 Scanning emails...'},
  {at:10000,pct:50,msg:'\uD83D\uDCE6 Importing RFQs...'},
  {at:20000,pct:65,msg:'\uD83D\uDCCA Pricing lookups...'},
  {at:35000,pct:75,msg:'\u23F3 Still working (large inbox)...'},
  {at:60000,pct:82,msg:'\u23F3 Hang tight, almost done...'},
  {at:90000,pct:88,msg:'\u23F3 This inbox has a lot of emails...'},
 ];
 var stepIdx=0;
 var timer=setInterval(function(){
  var el=Math.round((Date.now()-started)/1000);
  var tEl=document.getElementById('resync-timer');
  if(tEl)tEl.textContent=el+'s';
  while(stepIdx<steps.length&&(Date.now()-started)>=steps[stepIdx].at){
   pct=steps[stepIdx].pct;
   var sEl=document.getElementById('resync-status');
   if(sEl)sEl.textContent=steps[stepIdx].msg;
   var bEl=document.getElementById('resync-bar');
   if(bEl)bEl.style.width=pct+'%';
   stepIdx++;
  }
 },500);
 fetch('/api/resync',{credentials:'same-origin'}).then(function(r){return r.json()}).then(function(d){
  clearInterval(timer);
  _updatePollTime(d.last_check);
  var bEl=document.getElementById('resync-bar');
  if(bEl){bEl.style.width='100%';bEl.style.background='linear-gradient(90deg,#34d399,#10b981)';}
  var sEl=document.getElementById('resync-status');
  var parts=[];
  if(d.found>0)parts.push(d.found+' new');
  if(d.preserved>0)parts.push(d.preserved+' kept');
  if(d.pcs_preserved)parts.push(d.pcs_preserved+' PCs');
  var msg='\u2705 '+(parts.join(', ')||'Done');
  if(sEl)sEl.textContent=msg;
  btn.textContent=msg;
  btn.style.background='rgba(52,211,153,.2)';btn.style.borderColor='rgba(52,211,153,.4)';
  setTimeout(function(){location.reload()},1500);
 }).catch(function(e){
  clearInterval(timer);
  var sEl=document.getElementById('resync-status');
  if(sEl){sEl.textContent='\u274C Failed: '+(e.message||'timeout');sEl.style.color='#f87171';}
  var bEl=document.getElementById('resync-bar');
  if(bEl){bEl.style.background='#ef4444';}
  btn.textContent='\u274C Failed';btn.style.background='rgba(248,81,73,.15)';
  setTimeout(function(){
   var p=document.getElementById('resync-progress');if(p)p.remove();
   _resetBtn(btn,'\uD83D\uDD04 Resync');
  },4000);
 });
}

function _resetBtn(btn,label){
 btn.textContent=label;btn.disabled=false;btn.removeAttribute('aria-busy');
 btn.style.background='';btn.style.borderColor='';
}

// ── Notifications ──
(function initBell(){
  function updateBellCount(){
    fetch('/api/notifications/bell-count',{credentials:'same-origin'})
    .then(function(r){return r.json()}).then(function(d){
      var badge=document.getElementById('notif-badge');
      if(!badge||!d.ok)return;
      var total=d.total_badge||0;
      // Also fetch pending draft count and add to badge
      fetch('/api/outbox/pending-count',{credentials:'same-origin'})
      .then(function(r2){return r2.json()}).then(function(d2){
        var draftCount=(d2.ok&&d2.count)?d2.count:0;
        var combined=total+draftCount;
        if(combined>0){badge.textContent=combined>99?'99+':combined;badge.classList.add('show')}
        else{badge.classList.remove('show')}
      }).catch(function(){
        if(total>0){badge.textContent=total>99?'99+':total;badge.classList.add('show')}
        else{badge.classList.remove('show')}
      });
      var csEl=document.getElementById('notif-cs-count');
      if(csEl&&d.cs_drafts>0){csEl.textContent=d.cs_drafts+' CS draft(s)';csEl.style.display='inline'}
      else if(csEl){csEl.style.display='none'}
    }).catch(function(){});
  }
  updateBellCount();
  setInterval(updateBellCount,300000); // 5 min (was 30s)
})();
function toggleNotifPanel(){
  var panel=document.getElementById('notif-panel');
  if(!panel)return;
  panel.classList.toggle('open');
  if(panel.classList.contains('open')) loadNotifications();
}
function loadNotifications(){
  // Fetch pending drafts for inline outbox section
  fetch('/api/outbox/pending-count',{credentials:'same-origin'})
  .then(function(r){return r.json()}).then(function(d){
    var panel=document.getElementById('notif-panel');
    if(!panel||!d.ok||!d.count)return;
    // Remove existing draft section if any
    var existing=document.getElementById('notif-draft-section');
    if(existing)existing.remove();
    if(d.count>0){
      var draftSection=document.createElement('div');
      draftSection.id='notif-draft-section';
      draftSection.style.cssText='padding:8px 12px;border-bottom:1px solid var(--bd);background:rgba(210,153,34,.08)';
      draftSection.innerHTML='<div style="font-size:13px;font-weight:600;color:#d29922;margin-bottom:4px">' + d.count + ' draft(s) pending</div>'
        + '<a href="/outbox" style="font-size:12px;color:var(--ac)">Review in Outbox &rarr;</a>';
      var body=document.getElementById('notif-list');
      if(body&&body.parentNode)body.parentNode.insertBefore(draftSection,body);
    }
  }).catch(function(){});
  // Fetch regular notifications
  fetch('/api/notifications/persistent?limit=20',{credentials:'same-origin'})
  .then(function(r){return r.json()}).then(function(d){
    var list=document.getElementById('notif-list');
    if(!list)return;
    if(!d.notifications||d.notifications.length===0){
      list.innerHTML='<div class="notif-empty">No notifications yet.</div>';
      return;
    }
    var IC={urgent:'\uD83D\uDEA8',deal:'\uD83D\uDCB0',draft:'\uD83D\uDCCB',warning:'\u23F0',info:'\u2139\uFE0F'};
    list.innerHTML=d.notifications.map(function(n){
      var ts=n.created_at?new Date(n.created_at).toLocaleString(undefined,{month:'short',day:'numeric',hour:'numeric',minute:'2-digit',hour12:true}):'';
      var icon=IC[n.urgency]||'\uD83D\uDD14';
      return '<div class="notif-item '+(n.is_read?'':'unread')+' urgency-'+(n.urgency||'info')+'" onclick="notifClick(\''+n.deep_link+'\','+n.id+')">'
        +'<div class="notif-item-title">'+icon+' '+(n.title||'')+'</div>'
        +'<div class="notif-item-body">'+(n.body||'').substring(0,120)+'</div>'
        +'<div class="notif-item-time">'+ts+'</div>'
        +'</div>';
    }).join('');
  }).catch(function(){
    var list=document.getElementById('notif-list');
    if(list)list.innerHTML='<div class="notif-empty">Could not load notifications.</div>';
  });
}
function notifClick(link,id){
  if(id) fetch('/api/notifications/mark-read',{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json'},body:JSON.stringify({ids:[id]})});
  if(link&&link!=='/'){window.location.href=link}
  var p=document.getElementById('notif-panel');if(p)p.classList.remove('open');
}
function markAllRead(){
  fetch('/api/notifications/mark-read',{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json'},body:JSON.stringify({})})
  .then(function(){
    var b=document.getElementById('notif-badge');if(b)b.classList.remove('show');
    loadNotifications();
  });
}
document.addEventListener('click',function(e){
  var wrap=document.getElementById('notif-wrap');
  if(wrap&&!wrap.contains(e.target)){
    var panel=document.getElementById('notif-panel');
    if(panel)panel.classList.remove('open');
  }
});
// Format poll time on load
(function(){
 var el=document.getElementById('poll-time');
 if(el&&el.dataset.utc){
  try{var d=new Date(el.dataset.utc);if(!isNaN(d)){el.textContent=d.toLocaleString(undefined,{month:'short',day:'numeric',hour:'numeric',minute:'2-digit',hour12:true})}}catch(e){}
 }
})();

// ══════════════════════════════════════════════════════════════════════════
// FORCE REPROCESS: Clears ALL processed email UIDs and re-polls.
// Use when an email isn't being picked up despite code fixes.
// ══════════════════════════════════════════════════════════════════════════
function forceReprocess(btn){
 if(!confirm('FORCE REPROCESS:\n\n\u2022 Clears ALL processed email tracking\n\u2022 Re-scans entire inbox from scratch\n\u2022 May create duplicates of existing items\n\nUse ONLY when a specific email isn\'t being picked up.\nContinue?'))return;
 btn.disabled=true;btn.textContent='\uD83D\uDD27';
 btn.style.background='rgba(248,81,73,.15)';
 fetch('/api/force-reprocess',{method:'POST',credentials:'same-origin'}).then(function(r){return r.json()}).then(function(d){
  if(d.ok){
   btn.textContent='\u2705';btn.style.background='rgba(52,211,153,.2)';
   alert('Force reprocess complete:\n\u2022 Cleared '+d.cleared_uids+' tracked emails\n\u2022 Found '+d.found+' emails to import');
   if(d.found>0)setTimeout(function(){location.reload()},500);
   else setTimeout(function(){_resetBtn(btn,'\uD83D\uDD27')},2000);
  }else{
   alert('Error: '+(d.error||'unknown'));
   setTimeout(function(){_resetBtn(btn,'\uD83D\uDD27')},2000);
  }
 }).catch(function(){
  btn.textContent='\u274C';
  setTimeout(function(){_resetBtn(btn,'\uD83D\uDD27')},2000);
 });
}

// F8: Pricing Alerts Badge
(function(){
 try{
  fetch('/api/pricing-alerts',{credentials:'same-origin'}).then(function(r){return r.json()}).then(function(d){
   if(!d.ok||!d.total_alerts)return;
   var nav=document.querySelector('.nav-row2')||document.querySelector('nav')||document.querySelector('header');
   if(!nav)return;
   var badge=document.createElement('span');
   badge.id='pricing-alerts-badge';
   badge.style.cssText='background:#f85149;color:#fff;font-size:11px;font-weight:700;padding:2px 8px;border-radius:10px;cursor:pointer;margin-left:8px;display:inline-block';
   var parts=[];
   if(d.stale_rfqs.length)parts.push(d.stale_rfqs.length+' stale');
   if(d.unpriced_rfqs.length)parts.push(d.unpriced_rfqs.length+' unpriced');
   if(d.drift_items)parts.push(d.drift_items+' drifted');
   badge.textContent='\u26A0\uFE0F '+d.total_alerts;
   badge.title='Pricing alerts: '+parts.join(', ');
   badge.onclick=function(){
    var msg='PRICING ALERTS\n\n';
    if(d.stale_rfqs.length){msg+='Stale (>14 days):\n';d.stale_rfqs.forEach(function(s){msg+='  '+s.sol+' ('+s.age_days+'d)\n';});}
    if(d.unpriced_rfqs.length){msg+='\nUnpriced:\n';d.unpriced_rfqs.forEach(function(u){msg+='  '+u.sol+' ('+u.items+' items)\n';});}
    if(d.drift_items){msg+='\n'+d.drift_items+' items with >10% price drift in last 30 days';}
    alert(msg);
   };
   nav.appendChild(badge);
  }).catch(function(){});
 }catch(e){}
})();
