const PALETTE=[
  {bg:'rgba(29,185,84,.18)',color:'#1db954',border:'rgba(29,185,84,.35)'},
  {bg:'rgba(14,165,233,.18)',color:'#0ea5e9',border:'rgba(14,165,233,.35)'},
  {bg:'rgba(245,158,11,.18)',color:'#f59e0b',border:'rgba(245,158,11,.35)'},
  {bg:'rgba(239,68,68,.18)',color:'#ef4444',border:'rgba(239,68,68,.35)'},
  {bg:'rgba(168,85,247,.18)',color:'#a855f7',border:'rgba(168,85,247,.35)'},
  {bg:'rgba(236,72,153,.18)',color:'#ec4899',border:'rgba(236,72,153,.35)'},
  {bg:'rgba(20,184,166,.18)',color:'#14b8a6',border:'rgba(20,184,166,.35)'},
  {bg:'rgba(249,115,22,.18)',color:'#f97316',border:'rgba(249,115,22,.35)'},
  {bg:'rgba(132,204,22,.18)',color:'#84cc16',border:'rgba(132,204,22,.35)'},
  {bg:'rgba(6,182,212,.18)',color:'#06b6d4',border:'rgba(6,182,212,.35)'},
];
const audColorMap={};
let colorIdx=0;
function audColor(n){
  if(!audColorMap[n]) audColorMap[n]=PALETTE[colorIdx++%PALETTE.length];
  return audColorMap[n];
}

// ── ESTADO ──────────────────────────────────────────
let allRows=[];        // [{auditor, estacion, fecha}]
let stationsMap={};   // id → {name,lat,lng,totalDocks,barrio}
let nameToStation={}; // name.toLowerCase() → station
let weeks=[];
let activeWeeks=new Set();  // indices de semanas seleccionadas
let sortMode='docks_desc';
let searchVal='';
let audFilt='';        // nombre exacto del auditor seleccionado, o ''
let leafletMap=null;
let mapMarkers=[];
let logisticaReady=false;
let estacionesReady=false;

// ── CSV PARSER ──────────────────────────────────────
// Limpia BOM y null-bytes (artefactos UTF-16LE)
function cleanStr(s){
  return String(s).replace(/[\uFEFF\u200B\u00A0\x00]/g,'').trim();
}
function parseCSV(text){
  if(text.charCodeAt(0)===0xFEFF) text=text.slice(1);
  text=text.replace(/\x00/g,'');
  const lines=text.split(/\r?\n/).filter(l=>l.trim());
  if(lines.length<2) return {rows:[],headers:[]};
  const first=lines[0];
  let sep='\t';
  if(!first.includes('\t')) sep=first.split(';').length>first.split(',').length?';':',';
  const headers=splitLine(lines[0],sep).map(cleanStr);
  const rows=[];
  for(let i=1;i<lines.length;i++){
    const vals=splitLine(lines[i],sep);
    if(vals.every(v=>!v.trim())) continue;
    const obj={};
    headers.forEach((h,j)=>{ obj[h]=cleanStr(vals[j]||''); });
    rows.push(obj);
  }
  return {rows,headers};
}
function splitLine(line,sep){
  const res=[];let cur='',inQ=false;
  for(let i=0;i<line.length;i++){
    const c=line[i];
    if(c==='"'){inQ=!inQ;}
    else if(c===sep&&!inQ){res.push(cur);cur='';}
    else{cur+=c;}
  }
  res.push(cur);
  return res;
}

// ── FECHAS ──────────────────────────────────────────
function parseDate(s){
  if(!s) return null;
  let m=s.match(/^(\d{2})\/(\d{2})\/(\d{4})/);
  if(m) return new Date(+m[3],+m[2]-1,+m[1]);
  m=s.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if(m) return new Date(+m[1],+m[2]-1,+m[3]);
  return new Date(s);
}
function weekLabel(from,to){
  const f=d=>d.toLocaleDateString('es-AR',{day:'2-digit',month:'2-digit'});
  return f(from)+' – '+f(to);
}
function isoDate(d){
  return d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+String(d.getDate()).padStart(2,'0');
}
function getMon(d){
  const day=d.getDay(),diff=day===0?-6:1-day;
  const m=new Date(d);m.setDate(d.getDate()+diff);m.setHours(0,0,0,0);return m;
}

// ── PROGRESS ────────────────────────────────────────
function updateProgress(){
  const n=(logisticaReady?1:0)+(estacionesReady?1:0);
  document.getElementById('progress-fill').style.width=(n*50)+'%';
  document.getElementById('progress-label').textContent=n+' / 2 archivos cargados';
  if(n===2) launchApp();
}
function setCardDone(id,txt){
  const card=document.getElementById(id);
  card.classList.add('done');
  const st=card.querySelector('.uc-status');
  st.className='uc-status ok';
  st.textContent='✓ '+txt;
  card.querySelector('input').disabled=true;
}
function setCardErr(id,msg){
  document.getElementById(id).querySelector('.uc-status').className='uc-status err';
  document.getElementById(id).querySelector('.uc-status').textContent='✗ '+msg;
}

// ── PROCESAR ESTACIONES.CSV ─────────────────────────
function processEstaciones(text){
  const {rows,headers}=parseCSV(text);
  if(!rows.length){setCardErr('dz-estaciones','Archivo vacío');return;}

  const hLow=headers.map(h=>h.toLowerCase());
  const findH=(...kws)=>{
    for(const kw of kws){
      const i=hLow.findIndex(h=>h.includes(kw.toLowerCase()));
      if(i>=0) return headers[i];
    }
    return null;
  };

  const cId   =findH('id');
  const cName =findH('name','nombre');
  // Total de Anclajes es el nombre real en el CSV
  const cDocks=findH('total de anclajes','anclajes','total docks','total_docks');
  const cLat  =findH('latitude','latitud');
  const cLng  =findH('longitude','longitud');
  const cBar  =findH('barrio');

  if(!cId||!cName){setCardErr('dz-estaciones','No se encontraron columnas Id/Name');return;}

  stationsMap={};nameToStation={};
  let count=0;
  rows.forEach(r=>{
    const id=parseInt(r[cId]);
    if(!id||isNaN(id)) return;
    const name=(r[cName]||'').trim();
    const totalDocks=cDocks?parseInt(r[cDocks])||null:null;
    const lat=cLat?parseFloat(r[cLat])||null:null;
    const lng=cLng?parseFloat(r[cLng])||null:null;
    const barrio=(cBar?r[cBar]:'').trim();
    const obj={id,name,lat,lng,totalDocks,barrio};
    stationsMap[id]=obj;
    if(name) nameToStation[name.toLowerCase()]=obj;
    count++;
  });

  if(!count){setCardErr('dz-estaciones','Sin estaciones válidas');return;}
  estacionesReady=true;
  setCardDone('dz-estaciones',count+' estaciones');
  updateProgress();
}

// ── PROCESAR LOGISTICA.CSV ──────────────────────────
function processLogistica(text){
  const {rows,headers}=parseCSV(text);
  if(!rows.length){setCardErr('dz-logistica','Archivo vacío');return;}

  // Columnas exactas
  const C_APE  ='Apellido de ciclista';
  const C_NOM  ='Nombre de ciclista';
  const C_EST_I='Nombre de estación de inicio';
  const C_EST_F='Nombre de estación de fin de viaje';
  const C_ID_I ='Id de estación de inicio';
  const C_ID_F ='Id de estación de fin de viaje';
  const C_FECHA='Fecha de inicio';

  // Verificar que las columnas clave existen
  if(!headers.includes(C_APE)){setCardErr('dz-logistica','No se encontró columna "Apellido de ciclista"');return;}
  if(!headers.includes(C_FECHA)){setCardErr('dz-logistica','No se encontró columna "Fecha de inicio"');return;}

  // Filtrar auditores GOB + casos especiales (ej: "Audit Juan" Ramirez)
  const gobRows=rows.filter(r=>
    (r[C_APE]||'').toUpperCase().includes('GOB') ||
    (r[C_NOM]||'').toUpperCase().includes('AUDIT')
  );
  if(!gobRows.length){setCardErr('dz-logistica','No hay registros GOB');return;}

  allRows=[];
  gobRows.forEach(r=>{
    const idI=parseInt(r[C_ID_I])||0;
    const idF=parseInt(r[C_ID_F])||0;

    // Estación relevante — misma lógica que el Python
    let estacion;
    if([443,510].includes(idI))      estacion=r[C_EST_F]||r[C_EST_I];
    else if([443,510].includes(idF)) estacion=r[C_EST_I]||r[C_EST_F];
    else                             estacion=r[C_EST_I]||r[C_EST_F];

    // Nombre auditor: quitar GOB y guiones/espacios sobrantes
    const ape=(r[C_APE]||'').replace(/GOB/gi,'').replace(/[-\s]+$/,'').trim();
    const nomRaw=(r[C_NOM]||'').trim();
    // Si el nombre empieza con "Audit " (caso especial Ramirez), quitarlo
    const nom=nomRaw.replace(/^Audit\s+/i,'').trim();
    const auditor=nom&&ape ? nom+' '+ape : nom||ape||r[C_APE];

    const fecha=parseDate(r[C_FECHA]);
    if(!fecha||isNaN(fecha)) return;
    estacion=(estacion||'').trim();
    if(!estacion) return;

    allRows.push({auditor, estacion, fecha});
  });

  if(!allRows.length){setCardErr('dz-logistica','Sin filas válidas');return;}
  logisticaReady=true;
  setCardDone('dz-logistica',allRows.length.toLocaleString('es-AR')+' mov. GOB');
  updateProgress();
}

// ── LAUNCH APP ──────────────────────────────────────
function launchApp(){
  // Construir semanas
  const dates=allRows.map(r=>r.fecha);
  const minD=new Date(Math.min(...dates));
  const maxD=new Date(Math.max(...dates));
  weeks=[];
  let cur=getMon(minD);
  while(cur<=maxD){
    const sun=new Date(cur);sun.setDate(cur.getDate()+6);
    weeks.push({label:weekLabel(cur,sun),from:new Date(cur),to:new Date(sun)});
    cur.setDate(cur.getDate()+7);
  }
  activeWeeks=new Set([weeks.length-1]);

  // Badges
  const fmt=d=>d.toLocaleDateString('es-AR',{day:'2-digit',month:'2-digit',year:'numeric'});
  const bp=document.getElementById('badge-periodo');
  const bs=document.getElementById('badge-semanas');
  bp.textContent=fmt(minD)+' – '+fmt(maxD);bp.style.display='';
  bs.textContent=weeks.length+(weeks.length===1?' semana':' semanas');bs.style.display='';
  document.getElementById('kpi-total').textContent=allRows.length.toLocaleString('es-AR');
  document.getElementById('kpi-period-sub').textContent=fmt(minD)+' → '+fmt(maxD);

  // Poblar select con TODOS los auditores del período
  // (el filtro funciona sobre la semana pero el select muestra todos)
  const allAuditors=[...new Set(allRows.map(r=>r.auditor))].sort();
  const sel=document.getElementById('auditor-filter');
  sel.innerHTML='<option value="">Todos</option>';
  allAuditors.forEach(a=>{
    const o=document.createElement('option');
    o.value=a; o.textContent=a;
    sel.appendChild(o);
  });

  // Fade: ocultar upload, mostrar app
  const upWrap=document.getElementById('upload-screen').parentElement;
  upWrap.style.transition='opacity .35s';
  upWrap.style.opacity='0';
  setTimeout(()=>{
    upWrap.style.display='none';
    const app=document.getElementById('app');
    app.style.display='block';
    app.style.opacity='0';
    app.style.transition='opacity .35s';
    setTimeout(()=>{ app.style.opacity='1'; },20);
    initMap();
    renderWeekTabs();
    renderAll();
  },350);
}

// ── MAPA ────────────────────────────────────────────
function initMap(){
  if(leafletMap) return;
  leafletMap=L.map('map',{zoomControl:true}).setView([-34.615,-58.433],12);
  L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',{
    attribution:'© OpenStreetMap © CARTO',maxZoom:19
  }).addTo(leafletMap);
}
function updateMap(stRows){
  if(!leafletMap) return;
  mapMarkers.forEach(m=>m.marker.remove());
  mapMarkers=[];
  const auditedSet=new Set(stRows.map(r=>r.name.toLowerCase()));
  Object.values(stationsMap).forEach(st=>{
    if(!st.lat||!st.lng) return;
    const key=st.name.toLowerCase();
    const isAudited=auditedSet.has(key);
    const stRow=isAudited?stRows.find(r=>r.name.toLowerCase()===key):null;
    let color,radius,opacity;
    if(!isAudited){color='#ef4444';radius=4;opacity=0.4;}
    else{
      const pct=stRow&&st.totalDocks?stRow.total/st.totalDocks:null;
      if(pct===null){color='#8b949e';radius=7;opacity=0.85;}
      else if(pct>=0.75){color='#1db954';radius=8;opacity=1;}
      else if(pct>=0.4){color='#f59e0b';radius=7;opacity=0.9;}
      else{color='#ef4444';radius=6;opacity=0.85;}
    }
    const circle=L.circleMarker([st.lat,st.lng],{radius,color,fillColor:color,fillOpacity:opacity,weight:1.5,opacity:0.9});
    let html='<div class="map-popup"><h4>'+esc(st.name)+'</h4>';
    if(st.barrio) html+='<div class="mp-row">Barrio <span class="mp-val">'+esc(st.barrio)+'</span></div>';
    if(st.totalDocks) html+='<div class="mp-row">Total docks <span class="mp-val">'+st.totalDocks+'</span></div>';
    if(isAudited&&stRow){
      html+='<div class="mp-row">Auditados <span class="mp-val" style="color:#1db954">'+stRow.total+'</span></div>';
      if(st.totalDocks){
        const pct=(stRow.total/st.totalDocks*100).toFixed(1);
        const diff=st.totalDocks-stRow.total;
        html+='<div class="mp-row">Cobertura <span class="mp-val">'+pct+'%</span></div>';
        if(diff>0) html+='<div class="mp-row">Sin auditar <span class="mp-val" style="color:#ef4444">'+diff+'</span></div>';
      }
      html+='<div class="mp-row">Auditores <span class="mp-val">'+Object.keys(stRow.auditors).join(', ')+'</span></div>';
    } else {
      html+='<div class="mp-row" style="color:#ef4444">No auditada esta semana</div>';
    }
    html+='</div>';
    circle.bindPopup(html,{maxWidth:240});
    circle.addTo(leafletMap);
    mapMarkers.push({marker:circle,stName:key});
  });
}

// ── WEEK TABS ───────────────────────────────────────
function renderWeekTabs(){
  const cont=document.getElementById('week-tabs');
  cont.innerHTML='';
  // Botón "Todas"
  const btnAll=document.createElement('button');
  const allSelected=activeWeeks.size===weeks.length;
  btnAll.className='week-tab'+(allSelected?' active':'');
  btnAll.textContent='Todas';
  btnAll.onclick=()=>{
    if(allSelected){ activeWeeks=new Set([weeks.length-1]); }
    else { activeWeeks=new Set(weeks.map((_,i)=>i)); }
    renderWeekTabs(); renderAll();
  };
  cont.appendChild(btnAll);
  // Separador visual
  const sep=document.createElement('span');
  sep.style.cssText='display:inline-block;width:1px;height:22px;background:var(--border);margin:0 4px;vertical-align:middle;';
  cont.appendChild(sep);
  // Una por semana
  weeks.forEach((w,i)=>{
    const btn=document.createElement('button');
    const isActive=activeWeeks.has(i);
    btn.className='week-tab'+(isActive?' active':'');
    btn.textContent='S'+(i+1)+' · '+w.label;
    btn.onclick=(e)=>{
      if(e.shiftKey||e.ctrlKey||e.metaKey){
        // Shift/Ctrl: toggle individual
        if(activeWeeks.has(i)&&activeWeeks.size>1) activeWeeks.delete(i);
        else activeWeeks.add(i);
      } else {
        // Click normal: si ya está sola, deseleccionar y activar todas
        if(activeWeeks.size===1&&activeWeeks.has(i)){
          activeWeeks=new Set(weeks.map((_,j)=>j));
        } else {
          activeWeeks=new Set([i]);
        }
      }
      renderWeekTabs(); renderAll();
    };
    cont.appendChild(btn);
  });
}

// ── RENDER PRINCIPAL ────────────────────────────────
function renderAll(){
  // 1. Filtrar por semanas seleccionadas (puede ser 1 o más)
  const weekRanges=[...activeWeeks].map(i=>{
    const w=weeks[i];
    const wTo=new Date(w.to); wTo.setHours(23,59,59,999);
    return {from:w.from, to:wTo};
  });
  const weekRows=allRows.filter(r=>weekRanges.some(range=>r.fecha>=range.from&&r.fecha<=range.to));

  // Label para el badge del mapa
  const weekLabels=[...activeWeeks].sort().map(i=>'S'+(i+1));
  const weekLabelStr=activeWeeks.size===weeks.length?'Todas las semanas':weekLabels.join(', ');

  // 2. Filtrar por auditor (comparación directa de string)
  const filtered = audFilt
    ? weekRows.filter(r => r.auditor === audFilt)
    : weekRows;

  // 3. Filtrar por búsqueda de texto
  const searched = searchVal
    ? filtered.filter(r =>
        r.estacion.toLowerCase().includes(searchVal.toLowerCase()) ||
        r.auditor.toLowerCase().includes(searchVal.toLowerCase())
      )
    : filtered;

  // 4. Agrupar por estación
  const stMap={};
  searched.forEach(r=>{
    if(!stMap[r.estacion]) stMap[r.estacion]={name:r.estacion,auditors:{},total:0};
    stMap[r.estacion].total++;
    if(!stMap[r.estacion].auditors[r.auditor])
      stMap[r.estacion].auditors[r.auditor]={total:0,byDate:{}};
    stMap[r.estacion].auditors[r.auditor].total++;
    const dk=isoDate(r.fecha);
    stMap[r.estacion].auditors[r.auditor].byDate[dk]=
      (stMap[r.estacion].auditors[r.auditor].byDate[dk]||0)+1;
  });
  let stRows=Object.values(stMap);

  // 5. Enriquecer con datos de estaciones
  stRows.forEach(r=>{
    const st=nameToStation[r.name.toLowerCase()];
    r.stData=st||null;
    r.totalDocks=st?st.totalDocks:null;
    r.pct=r.totalDocks?r.total/r.totalDocks:null;
    r.diff=r.totalDocks!==null?r.totalDocks-r.total:null;
  });

  // 6. Ordenar
  stRows.sort((a,b)=>{
    if(sortMode==='docks_desc')  return b.total-a.total;
    if(sortMode==='docks_asc')   return a.total-b.total;
    if(sortMode==='pct_desc')    return (b.pct||0)-(a.pct||0);
    if(sortMode==='pct_asc')     return (a.pct||0)-(b.pct||0);
    if(sortMode==='name_asc')    return a.name.localeCompare(b.name,'es');
    if(sortMode==='diff_desc')   return (b.diff||0)-(a.diff||0);
    return b.total-a.total;
  });

  // 7. KPIs (siempre sobre weekRows completo, no filtrado)
  const uAuds=new Set(weekRows.map(r=>r.auditor));
  const uSts =new Set(weekRows.map(r=>r.estacion));
  const totalSts=Object.keys(stationsMap).length;
  const rowsPct=stRows.filter(r=>r.pct!==null);
  const avgPct=rowsPct.length
    ?(rowsPct.reduce((a,r)=>a+r.pct,0)/rowsPct.length*100).toFixed(1)+'%'
    :'N/D';

  document.getElementById('kpi-stations').textContent=uSts.size.toLocaleString('es-AR');
  document.getElementById('kpi-auditors').textContent=uAuds.size;
  document.getElementById('kpi-coverage').textContent=avgPct;
  document.getElementById('kpi-unvisited').textContent=(totalSts-uSts.size).toLocaleString('es-AR');
  document.getElementById('kpi-unvisited-sub').textContent='de '+totalSts+' totales';
  document.getElementById('kpi-week-sub').textContent=weekLabelStr;
  document.getElementById('kpi-cov-sub').textContent=rowsPct.length?'% promedio':'sin datos totales';
  document.getElementById('map-week-label').textContent=weekLabelStr;

  // Auditor cards y mapa (siempre semana completa, no filtrada)
  renderAuditorCards(weekRows);
  updateMap(stRows);

  // 8. Tabla
  const tbody=document.getElementById('main-tbody');
  const emptyMsg=document.getElementById('empty-msg');
  tbody.innerHTML='';
  document.getElementById('table-count').textContent=stRows.length+' estaciones';
  if(!stRows.length){ emptyMsg.classList.remove('hidden'); return; }
  emptyMsg.classList.add('hidden');

  stRows.forEach(r=>{
    const tr=document.createElement('tr');

    // Col 1: Estación + barrio
    let c1='<div class="td-station">'+esc(r.name)+'</div>';
    if(r.stData&&r.stData.barrio) c1+='<div class="td-station-sub">'+esc(r.stData.barrio)+'</div>';

    // Col 2: Auditores + desglose por fecha
    let c2='<div class="td-auditors">';
    Object.entries(r.auditors).sort((a,b)=>b[1].total-a[1].total).forEach(([an,ad])=>{
      const c=audColor(an);
      c2+='<span class="auditor-pill" style="background:'+c.bg+';color:'+c.color+';border:1px solid '+c.border+'">'+esc(an)+' '+ad.total+'</span>';
    });
    c2+='</div>';
    const allDates=[...new Set(Object.values(r.auditors).flatMap(ad=>Object.keys(ad.byDate)))].sort();
    if(allDates.length){
      c2+='<div class="dates-list">';
      allDates.forEach(dk=>{
        const dObj=new Date(dk+'T12:00:00');
        const dLbl=dObj.toLocaleDateString('es-AR',{weekday:'short',day:'2-digit',month:'2-digit'});
        c2+='<div class="date-row"><span class="date-tag">'+dLbl+'</span>';
        Object.entries(r.auditors).forEach(([an,ad])=>{
          const cnt=ad.byDate[dk]; if(!cnt) return;
          const c=audColor(an);
          c2+='<span class="date-ap" style="background:'+c.bg+';color:'+c.color+';border:1px solid '+c.border+'">'+esc(an)+'</span>';
          c2+='<span class="date-cnt" style="color:'+c.color+'">'+cnt+'</span>';
        });
        c2+='</div>';
      });
      c2+='</div>';
    }

    // Col 3: Auditados / Total + diff
    const tot=r.totalDocks!==null?r.totalDocks.toLocaleString('es-AR'):'—';
    let c3='<div style="text-align:right"><div class="cov-numbers" style="justify-content:flex-end">'
      +'<span class="cov-auditado" style="color:var(--accent)">'+r.total.toLocaleString('es-AR')+'</span>'
      +'<span class="cov-sep">/</span>'
      +'<span class="cov-total">'+tot+'</span></div>';
    if(r.diff!==null){
      const cls=r.diff<=0?'diff-ok':r.diff<=5?'diff-warn':'diff-pos';
      const lbl=r.diff>0?'−'+r.diff:r.diff===0?'✓':'';
      if(lbl) c3+='<span class="diff-badge '+cls+'">'+lbl+' docks</span>';
    }
    c3+='</div>';

    // Col 4: Barra cobertura
    let c4='<span style="font-family:var(--mono);font-size:10px;color:var(--muted)">—</span>';
    if(r.pct!==null){
      const pv=r.pct*100;
      const ps=pv.toFixed(1)+'%';
      const bc=pv>=75?'#1db954':pv>=40?'#f59e0b':'#ef4444';
      c4='<div><div class="cov-bar-bg"><div class="cov-bar-fill" style="width:'+Math.min(pv,100).toFixed(1)+'%;background:'+bc+'"></div></div>'
        +'<span class="cov-pct-label" style="color:'+bc+'">'+ps+'</span></div>';
    }

    tr.innerHTML='<td>'+c1+'</td><td>'+c2+'</td><td style="vertical-align:middle">'+c3+'</td><td style="vertical-align:middle">'+c4+'</td>';

    // Click → zoom en mapa
    tr.addEventListener('click',()=>{
      const st=r.stData;
      if(st&&st.lat&&leafletMap){
        leafletMap.setView([st.lat,st.lng],16);
        const m=mapMarkers.find(x=>x.stName===r.name.toLowerCase());
        if(m) m.marker.openPopup();
      }
    });
    tbody.appendChild(tr);
  });
}

// ── AUDITOR CARDS ───────────────────────────────────
function renderAuditorCards(weekRows){
  const byAud={};
  weekRows.forEach(r=>{
    if(!byAud[r.auditor]) byAud[r.auditor]={moves:0,stations:new Set()};
    byAud[r.auditor].moves++;
    byAud[r.auditor].stations.add(r.estacion);
  });
  const sorted=Object.entries(byAud).sort((a,b)=>b[1].moves-a[1].moves);
  const grid=document.getElementById('auditor-grid');
  grid.innerHTML='';
  sorted.forEach(([name,d])=>{
    const c=audColor(name);
    const div=document.createElement('div');
    div.className='auditor-card';
    div.style.borderColor=c.border;
    div.innerHTML='<div class="aud-name">'+esc(name)+'</div>'
      +'<div class="aud-moves" style="color:'+c.color+'">'+d.moves.toLocaleString('es-AR')+'</div>'
      +'<div class="aud-stations">'+d.stations.size+' estaciones</div>';
    grid.appendChild(div);
  });
}

// ── UTILS ───────────────────────────────────────────
function esc(s){
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}
function showToast(msg,type){
  const t=document.createElement('div');
  t.className='toast'+(type==='ok'?' toast-ok':'');
  t.textContent=msg;
  document.body.appendChild(t);
  setTimeout(()=>{ t.style.opacity='0'; setTimeout(()=>t.remove(),300); },3500);
}

// ── FILE HANDLING ───────────────────────────────────
// Para estaciones.csv: prioriza UTF-16LE (confirmado por el usuario)
// Para logistica.csv:  UTF-16LE → UTF-8 → latin-1
function handleFile(file,type){
  if(!file) return;
  const encs=type==='estaciones'
    ?['UTF-16LE','UTF-16BE','UTF-8','latin-1']
    :['UTF-16LE','UTF-8','latin-1'];

  function tryNext(i){
    if(i>=encs.length){
      setCardErr('dz-'+type,'No se pudo leer el archivo');
      return;
    }
    const r=new FileReader();
    r.onload=e=>{
      const text=e.target.result;
      const {headers}=parseCSV(text);
      // Validar que tiene al menos una columna esperada
      const valid = type==='logistica'
        ? headers.some(h=>h==='Apellido de ciclista')
        : headers.some(h=>/^id$/i.test(h));
      if(valid){
        type==='logistica' ? processLogistica(text) : processEstaciones(text);
      } else {
        tryNext(i+1);
      }
    };
    r.onerror=()=>tryNext(i+1);
    r.readAsText(file,encs[i]);
  }
  tryNext(0);
}

// ── EVENTOS ─────────────────────────────────────────
document.getElementById('input-logistica').addEventListener('change',e=>handleFile(e.target.files[0],'logistica'));
document.getElementById('input-estaciones').addEventListener('change',e=>handleFile(e.target.files[0],'estaciones'));

['dz-logistica','dz-estaciones'].forEach(id=>{
  const dz=document.getElementById(id);
  const type=id.split('-')[1];
  dz.addEventListener('dragover',e=>{e.preventDefault();dz.classList.add('drag');});
  dz.addEventListener('dragleave',()=>dz.classList.remove('drag'));
  dz.addEventListener('drop',e=>{
    e.preventDefault();dz.classList.remove('drag');
    handleFile(e.dataTransfer.files[0],type);
  });
});

// Filtros — lógica simple y directa
document.getElementById('search-input').addEventListener('input',e=>{
  searchVal=e.target.value;
  renderAll();
});
document.getElementById('auditor-filter').addEventListener('change',e=>{
  audFilt=e.target.value;   // valor exacto del option, igual al string en allRows
  renderAll();
});
document.getElementById('sort-select').addEventListener('change',e=>{
  sortMode=e.target.value;
  renderAll();
});