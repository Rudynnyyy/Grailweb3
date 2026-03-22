/**
 * backtest.js v3
 */
(function () {
  'use strict';
  const $ = id => document.getElementById(id);
  const btMarket=$('btMarket'),btStartDt=$('btStartDt'),btEndDt=$('btEndDt');
  const btHoldHours=$('btHoldHours'),btTopN=$('btTopN'),btFeeRate=$('btFeeRate');
  const btLeverage=$('btLeverage'),btDirection=$('btDirection');
  const btRunBtn=$('btRunBtn'),btCancelBtn=$('btCancelBtn'),btSyncBtn=$('btSyncBtn');
  const btProgressWrap=$('btProgressWrap'),btProgressText=$('btProgressText'),btProgressFill=$('btProgressFill');
  const btError=$('btError'),btCondBadges=$('btCondBadges'),btCondCount=$('btCondCount');
  const btStatsCard=$('btStatsCard'),btEquityCard=$('btEquityCard'),btSymbolsCard=$('btSymbolsCard');
  const btEquityChart=$('btEquityChart'),btSymbolsChart=$('btSymbolsChart');
  const btEquityPlaceholder=$('btEquityPlaceholder'),btSymbolsPlaceholder=$('btSymbolsPlaceholder');
  const btTopnBox=$('btTopnBox'),btTopnSortLabel=$('btTopnSortLabel'),btTopnN=$('btTopnN'),btTopnAllMsg=$('btTopnAllMsg');
  let currentTaskId=null,pollTimer=null,currentParams={},currentToggles={},currentFactors=[];
  const LS_PARAMS='qc_bt_params',LS_TOGGLES='qc_bt_toggles',LS_FACTORS='qc_bt_factors',LS_BT='qc_backtest_ui',LS_RESULT='qc_bt_last_result';

  function _saveResult(result){
    try{localStorage.setItem(LS_RESULT,JSON.stringify({ts:Date.now(),taskId:currentTaskId,result}));}catch(e){}
  }
  function _loadResult(){
    try{
      const raw=localStorage.getItem(LS_RESULT);
      if(!raw)return null;
      const obj=JSON.parse(raw);
      // 结果缓存1小时有效
      if(Date.now()-obj.ts>3600000){localStorage.removeItem(LS_RESULT);return null;}
      return obj;
    }catch(e){return null;}
  }

  const STEPS=[
    {id:'step_load',  icon:'\u{1F4C2}',name:'\u52a0\u8f7d\u5386\u53f2\u6570\u636e',desc:'\u8bfb\u53d6K\u7ebf CSV/PKL \u6587\u4ef6'},
    {id:'step_filter',icon:'\u{1F50D}',name:'\u9010Bar\u7b5b\u9009\u4fe1\u53f7',desc:'\u6700\u8017\u65f6\uff0c\u7ea6\u5360\u603b\u65f670%'},
    {id:'step_calc',  icon:'\u{1F4B0}',name:'\u8ba1\u7b97\u6301\u4ed3\u6536\u76ca',desc:'\u6309\u6301\u4ed3\u65f6\u957f\u6a21\u62df\u76c8\u4e8f'},
    {id:'step_stats', icon:'\u{1F4CA}',name:'\u7edf\u8ba1\u6307\u6807\u8ba1\u7b97',desc:'\u590f\u666e\u3001\u6700\u5927\u56de\u64a4\u7b49'},
    {id:'step_chart', icon:'\u{1F3A8}',name:'\u751f\u6210\u56de\u6d4b\u56fe\u8868',desc:'\u51c0\u503c\u66f2\u7ebf & \u5165\u9009\u9891\u6b21'},
  ];
  function buildStepsUI(){
    const host=$('btSteps');if(!host)return;
    host.innerHTML=STEPS.map(s=>`<div class="bt-step" id="${s.id}"><span class="bt-step-icon">${s.icon}</span><div class="bt-step-body"><div class="bt-step-name">${s.name}</div><div class="bt-step-desc">${s.desc}</div></div><div class="bt-step-right"><span class="bt-step-spinner"></span><span class="bt-step-badge">\u5b8c\u6210</span></div></div>`).join('');
  }
  function setStep(idx){STEPS.forEach((s,i)=>{const el=$(s.id);if(!el)return;el.classList.remove('active','done');if(i<idx)el.classList.add('done');else if(i===idx)el.classList.add('active');});}
  function inferStep(pct){if(pct<8)return 0;if(pct<72)return 1;if(pct<86)return 2;if(pct<95)return 3;return 4;}
  function showError(msg){btError.textContent=msg;btError.classList.add('active');}
  function hideError(){btError.classList.remove('active');}
  function setRunning(yes){
    btRunBtn.disabled=yes;btCancelBtn.classList.toggle('bt-hidden',!yes);btProgressWrap.classList.toggle('active',yes);
    if(!yes){btProgressFill.style.width='0%';btProgressText.textContent='';const pe=$('btProgressPct');if(pe)pe.textContent='0%';STEPS.forEach(s=>{const el=$(s.id);if(el)el.classList.remove('active','done');});}
  }
  function updateProgress(pct,msg){
    const p=Math.min(100,Math.max(0,pct));btProgressFill.style.width=p+'%';btProgressText.textContent=msg||'';
    const pe=$('btProgressPct');if(pe)pe.textContent=Math.round(p)+'%';setStep(inferStep(p));
  }
  let fakeProgressTimer=null,fakeProgressVal=0;
  function startFakeProgress(){
    fakeProgressVal=1;updateProgress(1,'\u6b63\u5728\u521d\u59cb\u5316...');clearInterval(fakeProgressTimer);
    fakeProgressTimer=setInterval(()=>{fakeProgressVal+=0.5;updateProgress(fakeProgressVal,'\u6b63\u5728\u52a0\u8f7d\u5386\u53f2\u6570\u636e...');
      if(fakeProgressVal>=8){clearInterval(fakeProgressTimer);
        fakeProgressTimer=setInterval(()=>{fakeProgressVal+=0.25;updateProgress(fakeProgressVal,'\u6b63\u5728\u9010Bar\u7b5b\u9009\u4fe1\u53f7\uff08\u6700\u8017\u65f6\uff0c\u8bf7\u8010\u5fc3\u7b49\u5f85\uff09...');
          if(fakeProgressVal>=68){clearInterval(fakeProgressTimer);
            fakeProgressTimer=setInterval(()=>{fakeProgressVal+=0.4;updateProgress(fakeProgressVal,'\u6b63\u5728\u8ba1\u7b97\u6301\u4ed3\u6536\u76ca...');
              if(fakeProgressVal>=85){clearInterval(fakeProgressTimer);fakeProgressTimer=null;}
            },400);
          }
        },800);
      }
    },300);
  }
  function stopFakeProgress(){clearInterval(fakeProgressTimer);fakeProgressTimer=null;}
  function _pad(n){return String(n).padStart(2,'0');}
  function _toLocalDtInput(d){return d.getFullYear()+'-'+_pad(d.getMonth()+1)+'-'+_pad(d.getDate())+'T'+_pad(d.getHours())+':'+_pad(d.getMinutes());}
  function initDefaultDates(){
    const saved=_loadBtUI();if(saved.start_dt){btStartDt.value=saved.start_dt;btEndDt.value=saved.end_dt||'';return;}
    const end=new Date();end.setMinutes(0,0,0);const start=new Date(end);start.setDate(start.getDate()-30);
    btStartDt.value=_toLocalDtInput(start);btEndDt.value=_toLocalDtInput(end);
  }
  function _saveBtUI(){
    const sk=$('btSortKey'),so=$('btSortOrder'),fc=$('btFullCoverage');
    try{localStorage.setItem(LS_BT,JSON.stringify({market:btMarket.value,start_dt:btStartDt.value,end_dt:btEndDt.value,hold_hours:btHoldHours.value,top_n:btTopN.value,fee_rate:btFeeRate.value,leverage:btLeverage?btLeverage.value:'1',direction:btDirection?btDirection.value:'long',sort_key:sk?sk.value:'pct_change',sort_order:so?so.value:'desc',full_coverage:!!(fc&&fc.checked)}));}catch(e){}
  }
  function _loadBtUI(){try{return JSON.parse(localStorage.getItem(LS_BT)||'{}');}catch(e){return {};}}
  function restoreBtUI(){
    const s=_loadBtUI();
    if(s.market)btMarket.value=s.market;
    if(s.start_dt)btStartDt.value=s.start_dt;
    if(s.end_dt)btEndDt.value=s.end_dt;
    if(s.hold_hours)btHoldHours.value=s.hold_hours;
    if(s.top_n!==undefined)btTopN.value=s.top_n;
    if(s.fee_rate)btFeeRate.value=s.fee_rate;
    if(s.leverage&&btLeverage)btLeverage.value=s.leverage;
    if(s.direction&&btDirection)btDirection.value=s.direction;
    const fc=$('btFullCoverage');if(fc&&s.full_coverage!==undefined)fc.checked=!!s.full_coverage;
  }

  // ── 排序选项 ────────────────────────────────────────────────────
  const SORT_OPTIONS=[
    {value:'pct_change',  label:'上期涨跌幅'},
    {value:'close',       label:'收盘价'},
    {value:'volume',      label:'成交量'},
    {value:'quote_volume',label:'成交额'},
    {value:'ma_5',        label:'MA(5)'},
    {value:'ma_10',       label:'MA(10)'},
    {value:'ma_20',       label:'MA(20)'},
    {value:'ma_60',       label:'MA(60)'},
    {value:'rsi_6',       label:'RSI(6)'},
    {value:'rsi_14',      label:'RSI(14)'},
    {value:'ema',         label:'EMA'},
    {value:'kdj_k',       label:'KDJ K'},
    {value:'obv',         label:'OBV'},
    {value:'stoch_rsi_k', label:'StochRSI K'},
  ];

  function buildSortUI(){
    if($('btSortKey'))return;
    const box=$('btTopnBox');if(!box)return;
    const row=document.createElement('div');
    row.style.cssText='display:flex;align-items:center;gap:8px;margin-top:10px;flex-wrap:wrap';
    const selStyle='padding:6px 10px;border:1px solid var(--border);border-radius:7px;font-size:12px;background:var(--input-bg);color:var(--text);outline:none;font-family:var(--sans)';
    const opts=SORT_OPTIONS.map(o=>`<option value="${o.value}">${o.label}</option>`).join('');
    row.innerHTML=`<label style="font-size:11px;color:var(--text-sub);font-weight:600;white-space:nowrap">排序字段</label><select id="btSortKey" style="flex:1;min-width:130px;${selStyle}">${opts}</select><select id="btSortOrder" style="${selStyle}"><option value="desc">降序 ↓</option><option value="asc">升序 ↑</option></select>`;
    box.appendChild(row);
    const saved=_loadBtUI();
    const sk=$('btSortKey'),so=$('btSortOrder');
    if(sk&&saved.sort_key)sk.value=saved.sort_key;
    if(so&&saved.sort_order)so.value=saved.sort_order;
    if(sk)sk.addEventListener('change',updateTopnBox);
    if(so)so.addEventListener('change',updateTopnBox);
  }
  function loadConditionsFromStorage(){
    try{currentParams=JSON.parse(localStorage.getItem(LS_PARAMS)||'{}');currentToggles=JSON.parse(localStorage.getItem(LS_TOGGLES)||'{}');currentFactors=JSON.parse(localStorage.getItem(LS_FACTORS)||'[]');}
    catch(e){currentParams={};currentToggles={};currentFactors=[];}
    renderCondBadges();updateTopnBox();
  }
  function buildCondLabel(key,params){
    const p=params||{};
    switch(key){
      case 'condCloseMa': return `close > MA(${p.maPeriodClose||20})`;
      case 'condMa':      return `MA(${p.maFast||10}) > MA(${p.maSlow||20})`;
      case 'condRsi':{const thr=(p.rsiThreshold!==undefined&&p.rsiThreshold!==null)?p.rsiThreshold:60;return `RSI(${p.rsiPeriod||14}) > ${thr}`;}
      case 'condEma':      return `close > EMA(${p.emaPeriod||20})`;
      case 'condBollUp':   return `close > BOLL\u2191(${p.bollPeriod||20},${p.bollStd||2}\u03c3)`;
      case 'condBollDown': return `close < BOLL\u2193(${p.bollDownPeriod||20},${p.bollDownStd||2}\u03c3)`;
      case 'condSuper':    return `close > ST(${p.superAtrPeriod||10},${p.superMult||3})`;
      case 'condKdj':      return `KDJ(${p.kdjN||9},${p.kdjM1||3},${p.kdjM2||3}) K>D`;
      case 'condObv':      return `OBV > MA(${p.obvMaPeriod||20})`;
      case 'condStochRsi': return `StochRSI(${p.stochRsiP||14},${p.stochRsiK||14}) K>D`;
      default: return key;
    }
  }
  function renderCondBadges(){
    const active=Object.entries(currentToggles).filter(([,v])=>v).map(([k])=>k);
    const af=(currentFactors||[]).filter(f=>f.enabled);
    if(btCondCount)btCondCount.textContent=(active.length+af.length)+' 条';
    if(!active.length&&!af.length){
      btCondBadges.innerHTML='<span class="bt-cond-empty">未读取到筛选条件，请先在主页面配置后点击「从主页面同步条件」</span>';
      return;
    }
    let html='';
    active.forEach(k=>{html+=`<span class="bt-tag">${buildCondLabel(k,currentParams)}</span>`;});
    af.forEach(f=>{
      let label=f.name||f.id||'自定义因子';
      if(f.thresholdEnabled&&f.threshold!==undefined)label+=` ${f.cmp||'>='} ${f.threshold}`;
      html+=`<span class="bt-tag factor">${label}</span>`;
    });
    btCondBadges.innerHTML=html;
  }
  function getSortLabel(key){
    const opt=SORT_OPTIONS.find(o=>o.value===key);
    if(opt)return opt.label;
    if(key&&key.startsWith('rsi_'))return `RSI(${key.split('_')[1]})`;
    if(key&&key.startsWith('ma_'))return `MA(${key.split('_')[1]})`;
    if(key&&key.startsWith('expr_'))return '自定义因子';
    return key||'上期涨跌幅';
  }
  function updateTopnBox(){
    if(!btTopnBox)return;
    const topN=parseInt(btTopN?btTopN.value:'0')||0;
    const sk=$('btSortKey'),so=$('btSortOrder');
    const sortKey=sk?sk.value:'pct_change';
    const sortOrder=so?so.value:'desc';
    const arrow=sortOrder==='asc'?'↑':'↓';
    if(btTopnSortLabel)btTopnSortLabel.textContent=getSortLabel(sortKey)+arrow;
    if(btTopnN)btTopnN.textContent=topN>0?topN:'全部';
    btTopnBox.classList.add('visible');
    if(btTopnAllMsg)btTopnAllMsg.classList.toggle('visible',topN<=0);
  }
  function buildConfig(){
    function toISO(v){if(!v)return '';const d=new Date(v);return isNaN(d.getTime())?v:d.toISOString();}
    const sk=$('btSortKey'),so=$('btSortOrder'),fc=$('btFullCoverage');
    return{
      params:currentParams,toggles:currentToggles,custom_factors:currentFactors,
      market:btMarket.value||'swap',start_dt:toISO(btStartDt.value),end_dt:toISO(btEndDt.value),
      hold_hours:Math.max(1,parseInt(btHoldHours.value)||1),
      top_n:Math.max(0,parseInt(btTopN.value)||0),
      fee_rate:Math.max(0,parseFloat(btFeeRate.value)||0.0005),
      leverage:Math.max(0.1,parseFloat((btLeverage&&btLeverage.value)||'1')||1),
      direction:(btDirection&&btDirection.value)||'long',
      sort_key:(sk&&sk.value)||'pct_change',
      sort_order:(so&&so.value)||'desc',
      full_coverage:!!(fc&&fc.checked),
    };
  }
  function hideResults(){[btStatsCard,btEquityCard,btSymbolsCard].forEach(el=>{if(el)el.classList.add('bt-hidden');});}
  async function runBacktest(){
    hideError();
    const config=buildConfig();
    if(!config.start_dt||!config.end_dt){showError('请设置开始时间和结束时间');return;}
    if(new Date(config.start_dt)>=new Date(config.end_dt)){showError('开始时间必须早于结束时间');return;}
    // hold_hours=1时两种模式完全一致，给用户提示
    const fc=$('btFullCoverage');
    if(config.hold_hours===1&&fc&&!fc.checked){
      fc.checked=true;config.full_coverage=true;
      showError('提示：持仓Bar数=1时，「每小时独立开仓」与「无重叠」效果完全相同，已自动勾选全覆盖模式。');
    }
    // 现货不支持做空
    if(config.direction==='short'&&(config.market==='spot'||config.market==='all')){
      showError('现货市场不支持做空，请选择「合约(swap)」市场，或将方向改为「做多」');
      return;
    }
    const hasT=Object.values(currentToggles).some(v=>v);
    if(!hasT){currentToggles={condCloseMa:true};if(!currentParams.maPeriodClose)currentParams.maPeriodClose=20;renderCondBadges();}
    _saveBtUI();setRunning(true);hideResults();startFakeProgress();
    try{
      const resp=await fetch('/api/backtest/run',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(config)});
      const data=await resp.json();
      if(!data.ok){stopFakeProgress();setRunning(false);showError(data.error||'提交失败');return;}
      currentTaskId=data.task_id;pollStatus();
    }catch(e){stopFakeProgress();setRunning(false);showError('请求失败：'+e.message);}
  }
  function pollStatus(){
    clearTimeout(pollTimer);
    pollTimer=setTimeout(async()=>{
      if(!currentTaskId)return;
      try{
        const resp=await fetch('/api/backtest/status?task_id='+encodeURIComponent(currentTaskId));
        const data=await resp.json();
        if(!data.ok){stopFakeProgress();setRunning(false);showError(data.error||'状态查询失败');return;}
        const rp=parseInt(data.progress)||0;
        if(rp>fakeProgressVal){stopFakeProgress();updateProgress(rp,data.message||'');}
        if(data.status==='done'){stopFakeProgress();updateProgress(100,'回测完成！');setTimeout(()=>{setRunning(false);fetchResult();},500);}
        else if(data.status==='failed'){stopFakeProgress();setRunning(false);showError('回测失败：'+(data.error||data.message||'未知错误'));}
        else if(data.status==='cancelled'){stopFakeProgress();setRunning(false);showError('回测已取消');}
        else pollStatus();
      }catch(e){pollStatus();}
    },1200);
  }
  async function fetchResult(){
    if(!currentTaskId)return;
    try{
      const resp=await fetch('/api/backtest/result?task_id='+encodeURIComponent(currentTaskId));
      const data=await resp.json();
      if(!data.ok||!data.result){showError('结果读取失败');return;}
      renderResult(data.result);
    }catch(e){showError('结果请求异常：'+e.message);}
  }
  function renderResult(result){
    _saveResult(result);
    const stats=result.stats||{},cfg=result.config_snapshot||{};
    const sumEl=$('btConfigSummary');
    if(sumEl){
      const mkt=cfg.market||'?',sd=(cfg.start_dt||'').slice(0,10),ed=(cfg.end_dt||'').slice(0,10);
      const hn=cfg.hold_hours||1,tn=cfg.top_n||0,fee=((cfg.fee_rate||0.0005)*100).toFixed(3);
      const lev=cfg.leverage||1,dir=cfg.direction==='short'?'做空':'做多';
      sumEl.innerHTML=[`<span>${mkt}</span> | ${sd} ~ ${ed}`,`持仓 <span>${hn}h</span> | TopN=<span>${tn||'全部'}</span>`,`手续费 <span>${fee}%</span> | 杠杆 <span>${lev}x</span> | <span>${dir}</span>`].join('<br>');
    }
    const grid=$('btStatsGrid');
    if(grid){
      const DEFS=[
        {key:'total_return',      label:'总收益',    pct:true},
        {key:'annualized_return', label:'年化收益',  pct:true},
        {key:'sharpe_ratio',      label:'夏普比率',  pct:false},
        {key:'max_drawdown',      label:'最大回撤',  pct:true},
        {key:'win_rate',          label:'胜率',      pct:true},
        {key:'calmar_ratio',      label:'卡玛比率',  pct:false},
        {key:'total_trades',      label:'总交易次数',pct:false},
      ];
      function _fmt(val,pct){
        if(val===null||val===undefined)return '—';
        const n=Number(val);
        if(!isFinite(n)||isNaN(n))return '—';
        if(pct){
          // 年化等百分比：先限幅再格式化，避免科学计数法
          const clamped=Math.max(-9999,Math.min(9999,n*100));
          return clamped.toFixed(2)+'%';
        }
        if(Number.isInteger(n))return String(n);
        return Math.max(-99999,Math.min(99999,n)).toFixed(2);
      }
      grid.innerHTML=DEFS.map(def=>{
        const raw=stats[def.key],val=(raw!==undefined&&raw!==null)?raw:null;
        const display=_fmt(val,def.pct);
        let cls='';
        if(val!==null&&typeof val==='number'&&isFinite(val)){
          if(def.key==='max_drawdown')cls=val>0?'neg':'';
          else if(def.key!=='total_trades')cls=val>0?'pos':(val<0?'neg':'');
        }
        return `<div class="bt-stat"><div class="bt-stat-label">${def.label}</div><div class="bt-stat-value ${cls}">${display}</div></div>`;
      }).join('');
    }
    if(btStatsCard)btStatsCard.classList.remove('bt-hidden');
    if(btEquityCard){
      btEquityCard.classList.remove('bt-hidden');
      if(btEquityChart&&btEquityPlaceholder){
        const src='/api/backtest/chart/equity?task_id='+encodeURIComponent(currentTaskId)+'&t='+Date.now();
        btEquityChart.onload=()=>{btEquityPlaceholder.classList.add('bt-hidden');btEquityChart.classList.remove('bt-hidden');};
        btEquityChart.onerror=()=>{btEquityPlaceholder.textContent='图表生成失败';};
        btEquityChart.src=src;
      }
    }
    if(btSymbolsCard){
      btSymbolsCard.classList.remove('bt-hidden');
      if(btSymbolsChart&&btSymbolsPlaceholder){
        const src='/api/backtest/chart/symbols?task_id='+encodeURIComponent(currentTaskId)+'&t='+Date.now();
        btSymbolsChart.onload=()=>{btSymbolsPlaceholder.classList.add('bt-hidden');btSymbolsChart.classList.remove('bt-hidden');};
        btSymbolsChart.onerror=()=>{btSymbolsPlaceholder.textContent='图表生成失败';};
        btSymbolsChart.src=src;
      }
    }
  }
  async function cancelBacktest(){
    if(!currentTaskId)return;
    try{await fetch('/api/backtest/cancel',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({task_id:currentTaskId})});}catch(e){}
    stopFakeProgress();setRunning(false);clearTimeout(pollTimer);currentTaskId=null;showError('回测已取消');
  }
  function initTheme(){
    const saved=localStorage.getItem('qc_theme');
    if(saved==='light')document.documentElement.classList.add('light');
    const btn=$('btThemeBtn');
    if(btn)btn.addEventListener('click',()=>{const isLight=document.documentElement.classList.toggle('light');localStorage.setItem('qc_theme',isLight?'light':'dark');});
  }
  function init(){
    initTheme();buildStepsUI();buildSortUI();restoreBtUI();initDefaultDates();loadConditionsFromStorage();
    if(btRunBtn)btRunBtn.addEventListener('click',runBacktest);
    if(btCancelBtn)btCancelBtn.addEventListener('click',cancelBacktest);
    if(btSyncBtn)btSyncBtn.addEventListener('click',()=>{loadConditionsFromStorage();});
    if(btTopN)btTopN.addEventListener('input',updateTopnBox);
    // 市场或方向变化时实时提示做空限制
    if(btMarket)btMarket.addEventListener('change',()=>{
      const dir=btDirection&&btDirection.value;
      const mkt=btMarket.value;
      if(dir==='short'&&(mkt==='spot'||mkt==='all')){
        showError('提示：现货市场不支持做空，请切换方向为「做多」或选择「合约(swap)」市场');
      }else{hideError();}
    });
    if(btDirection)btDirection.addEventListener('change',()=>{
      const dir=btDirection.value;
      const mkt=btMarket&&btMarket.value;
      if(dir==='short'&&(mkt==='spot'||mkt==='all')){
        showError('提示：现货市场不支持做空，请切换市场为「合约(swap)」');
      }else{hideError();}
    });
    // 恢复上次回测结果缓存
    const cached=_loadResult();
    if(cached&&cached.result){
      currentTaskId=cached.taskId||null;
      try{renderResult(cached.result);}catch(e){}
    }
  }
  if(document.readyState==='loading'){document.addEventListener('DOMContentLoaded',init);}else{init();}
})();
