import {useEffect,useState} from 'react';import {researchApi} from '../api/research';import {SHARD_STATUS} from '../constants/enums';import {fmt} from '../utils/format';import type {RunProgress} from '../types/domain';
export function ShardProgress({run,user,reload}:{run:any;user:any;reload:()=>void}){
 const [p,P]=useState<RunProgress|null>(null),[err,E]=useState(''),[busy,B]=useState(false),[note,N]=useState('');
 const load=async()=>{try{P(await researchApi.runProgress(run.id))}catch(e:any){E(e.message)}};
 useEffect(()=>{P(null);load();const t=setInterval(load,2500);return()=>clearInterval(t)},[run.id]);
 const resume=async()=>{B(true);E('');N('');try{const r=await researchApi.resumeRun(run.id);N(`已重新投递 ${r.dispatched==='plan'?'规划任务':r.dispatched+' 个分片'}，回收失联租约 ${r.reset_leases} 个`);await load();reload()}catch(e:any){E(e.message)}finally{B(false)}};
 const pct=p&&p.permutations?Math.min(100,100*p.done_permutations/p.permutations):0;
 return <section className="section shard-progress"><div className="section-head"><h2>分片置换进行中，进度已落库。</h2><span>{p?`${fmt(p.done_permutations,0)} / ${fmt(p.permutations,0)} 次置换`:'读取进度…'}</span></div>
 <p>置换按分片并行计算；中断、重启或执行者失联后从已保存的分片继续，已完成的置换不重复计算。全部分片齐备前，不发布全谱与结论。</p>
 <div className="progress-track" role="progressbar" aria-valuenow={Math.round(pct)} aria-valuemin={0} aria-valuemax={100}><div className="progress-fill" style={{width:pct+'%'}}/></div>
 {p&&p.total_shards>0&&<>
  <div className="shard-stats"><span>分片 {p.done_shards}/{p.total_shards} 已完成</span><span>{p.leased_shards} 执行中 / {p.pending_shards} 待领取</span>{p.shard_size&&<span>每片 {fmt(p.shard_size,0)} 次</span>}</div>
  <div className="shard-grid">{p.shards.slice(0,400).map(s=><i key={s.index} className={'shard-cell '+s.status} title={`#${s.index} ${SHARD_STATUS[s.status]||s.status} / ${fmt(s.count,0)}次 / 尝试${s.attempts}${s.lease_owner?' / '+s.lease_owner:''}`}/>)}{p.total_shards>400&&<small>… 共{p.total_shards}片，仅示前400</small>}</div>
  <div className="shard-legend">{Object.entries(SHARD_STATUS).map(([k,v])=><span key={k}><i className={'shard-cell '+k}/>{v}</span>)}</div>
 </>}
 {p&&p.total_shards===0&&<p>规划任务尚未到达：测点与计划已在排队时冻结，worker 空闲后即生成分片。</p>}
 {note&&<p className="ok-text">{note}</p>}{err&&<p className="error-text">{err}</p>}
 {user.role!=='viewer'&&<button className="primary" disabled={busy} onClick={resume}>恢复任务 / 接管失联分片 ↻</button>}
 </section>}
