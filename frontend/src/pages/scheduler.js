import {api} from '../api/client.js';
import {scheduler} from '../api/entities.js';
import {session} from '../stores/session.js';
import {heading} from '../components/common/shell.js';
import {esc,date,notify} from '../utils/dom.js';
import {submit} from '../hooks/forms.js';
export async function schedulerPage(){
 const [s,audits]=await Promise.all([scheduler.get(),api('/audits')]);
 const events=audits.filter(a=>/^(job|scheduler)\./.test(a.action)).slice(0,8);
 return heading('04 / DISPATCH','执行名额与候算队列','全台执行名额限制并发计算；候算任务按负责人轮转占位，新来短队也能轮到。已有运行不抢停，名额缩减后待空位再放行。','<button class="button" id="refresh-scheduler">刷新队列 ↻</button>')
 +'<section class="overview-hero"><div class="narrative"><p class="eyebrow">全台执行名额</p><h2>'+(s.overcommitted?'名额已缩减至 '+s.capacity+'，现有 '+s.running+' 个运行继续执行，待空位释放后再放行新任务':'当前 '+s.running+' / '+s.capacity+' 个名额占位中，'+s.queued+' 个任务候算')+'</h2><p>名额由业务负责人调整'+(s.updated_by?'，最近由 '+esc(s.updated_by)+' 调整':'')+'。进程失联后租约到期可被接管，接管原因随任务留痕。</p></div><div class="numbers">'+[['执行名额',s.capacity],['占位执行',s.running],['候算中',s.queued]].map(([k,v])=>'<div><span>'+k+'</span><strong>'+v+'</strong></div>').join('')+'</div></section>'
 +(session.user.role==='admin'?'<section class="panel"><div class="section-title"><h2>调整执行名额</h2><span>仅业务负责人</span></div><form id="capacity-form" class="capacity-form"><label>全台执行名额（1–16）<input type="number" name="capacity" min="1" max="16" value="'+s.capacity+'" required></label><button type="submit" class="primary">保存名额</button></form><p class="hint">已有运行不抢停；缩减名额后，待运行自然释放空位再放行新任务。</p></section>':'')
 +'<div class="two-columns"><section class="panel"><div class="section-title"><h2>占位执行</h2><span>租约 '+s.lease_seconds+' 秒 · 心跳续租</span></div>'
 +(s.slots.length?'<div class="slot-list">'+s.slots.map(x=>'<article class="slot-card"><strong>#'+x.job_id+' '+esc(x.name)+'</strong><p>'+esc(x.owner)+' · 进程 '+esc(x.worker_id)+'</p><small>第 '+x.attempts+' 次尝试 · 租约剩余 '+x.lease_remaining+' 秒</small>'+(x.takeover_reason?'<p class="takeover">接管原因：'+esc(x.takeover_reason)+'</p>':'')+'</article>').join('')+'</div>':'<p class="empty">当前没有占位执行的任务。</p>')
 +'</section><section class="panel"><div class="section-title"><h2>候算队列</h2><span>按负责人轮转的预计顺位</span></div>'
 +(s.queue.length?'<div class="table-wrap"><table><thead><tr><th>顺位</th><th>任务</th><th>负责人</th><th>入队时间</th></tr></thead><tbody>'+s.queue.map(q=>'<tr><td>'+q.position+'</td><td>#'+q.job_id+' '+esc(q.name)+'</td><td>'+esc(q.owner)+'</td><td>'+date(q.created_at)+'</td></tr>').join('')+'</tbody></table></div>':'<p class="empty">候算队列为空，新提交的分析任务会按负责人轮转进入派发。</p>')
 +'</section></div>'
 +'<section class="panel"><div class="section-title"><h2>调度留痕</h2><span>领取、接管与回写记录</span></div><ul class="timeline">'+(events.length?events.map(a=>'<li><strong>'+esc(a.action)+'</strong><span>'+esc(a.actor)+' · '+date(a.at)+'</span></li>').join(''):'<li><strong>暂无调度事件</strong><span>任务执行后在此留痕</span></li>')+'</ul></section>';
}
export function bindScheduler(refresh){
 document.querySelector('#refresh-scheduler')?.addEventListener('click',refresh);
 const form=document.querySelector('#capacity-form');
 if(form)submit(form,async f=>{await scheduler.setCapacity(Number(f.get('capacity')));notify('执行名额已更新');await refresh();});
}
