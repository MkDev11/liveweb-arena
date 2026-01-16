#!/usr/bin/env python3
"""快速生成测试覆盖报告"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from liveweb_arena.core.validators.base import get_registered_templates
import liveweb_arena.plugins.taostats.templates
import liveweb_arena.plugins.weather.templates

templates = get_registered_templates()

print("=" * 80)
print("测试用例覆盖报告")
print("=" * 80)

all_test_cases = []

for name in sorted(templates.keys()):
    cls = templates[name]
    template = cls()

    metrics_found = {}
    for seed in range(0, 300):
        try:
            q = template.generate(seed)
            vi = q.validation_info

            if 'metric' in vi:
                key = vi['metric']
            elif 'change_type' in vi:
                key = vi['change_type']
            elif 'comparison_type' in vi:
                key = vi['comparison_type']
            elif 'ranking_metric' in vi:
                key = f"ranking_{vi['ranking_metric']}"
            else:
                key = 'default'

            if key not in metrics_found:
                metrics_found[key] = {
                    'seed': seed,
                    'question': q.question_text,
                }
                all_test_cases.append({
                    'template': name,
                    'metric': key,
                    'seed': seed,
                    'question': q.question_text,
                })
        except:
            pass

    print(f"\n### {name} ({len(metrics_found)} 指标)")
    for metric, info in sorted(metrics_found.items()):
        print(f"  [{metric}] seed={info['seed']}")
        print(f"    {info['question'][:70]}...")

print(f"\n\n总计: {len(all_test_cases)} 个测试用例")
print("\n推荐测试 seeds:")
for tc in all_test_cases:
    print(f"  {tc['template']}:{tc['metric']} -> seed={tc['seed']}")
