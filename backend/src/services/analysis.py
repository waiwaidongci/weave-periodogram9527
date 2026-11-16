from .validation import validate_points,validate_plan
from .qr_cache import build_cache
from .spectrum import observed_spectrum
from .permutation import global_permutation,null_test
from .fingerprint import fingerprint

TIE_RULE='lowest frequency within absolute power tolerance 1e-12 of maximum; upper-tail null ties use same tolerance'
INTERPRETATION='Frozen predeclared frequency grid. Sampling window, aliasing, leakage and grid spacing limit interpretation; the selected period is not a confidence interval or a manufacturing cause. Independent homoscedastic exchangeable errors are required.'

def observed_base(study,points,plan):
 # 观测谱与冻结元数据：串行与分片两条路径共用的同一份中间结果（除global_test外的全部字段）
 points=validate_points(points);f=validate_plan(plan);x=[p['position'] for p in points];y=[p['thickness'] for p in points];cache=build_cache(x,f);result,centered=observed_spectrum(cache,y)
 staging={**result,'source_study':study,'plan':plan,'n':len(points),'coordinate_origin':cache['origin'],'qr_cache_builds':1,'fingerprint':fingerprint(study,points,plan),'tie_rule':TIE_RULE,'interpretation':INTERPRETATION}
 return staging,centered,cache

def analyze(study,points,plan):
 staging,centered,cache=observed_base(study,points,plan);test=global_permutation(cache,centered,staging['rss0'],staging['observed_max_power'],plan['permutations'],plan['seed'])
 return {**staging,'global_test':test}

def finalize_result(staging,null_maxima,seed):
 # 分片路径：全部null最大值按分片序号拼接齐备后，得到与串行完全一致的global_test与全谱结论
 return {**staging,'global_test':null_test(null_maxima,staging['observed_max_power'],seed)}
