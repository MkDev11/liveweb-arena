#!/usr/bin/env python3
"""
系统化测试脚本：覆盖所有模板的所有指标

使用方法：
    # 列出所有测试用例
    python scripts/test_all_templates.py --list

    # 运行所有测试
    python scripts/test_all_templates.py --run

    # 运行特定模板的测试
    python scripts/test_all_templates.py --run --template taostats_validator

    # 只生成问题，不运行 agent
    python scripts/test_all_templates.py --generate
"""

import argparse
import asyncio
import json
import random
import sys
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from liveweb_arena.core.validators.base import get_registered_templates
import liveweb_arena.plugins.taostats.templates  # noqa
import liveweb_arena.plugins.weather.templates  # noqa


@dataclass
class TestCase:
    """测试用例"""
    template_name: str
    metric: str  # 指标名称
    seed: int
    question: str
    expected_type: str  # 期望的答案类型描述


@dataclass
class TestResult:
    """测试结果"""
    test_case: TestCase
    passed: bool
    actual_answer: Optional[str]
    score: float
    reasoning: str
    time_taken: float
    error: Optional[str] = None


def discover_test_cases() -> List[TestCase]:
    """
    发现所有需要测试的用例

    策略：对每个模板，找到覆盖所有指标的最小 seed 集合
    """
    test_cases = []
    registered = get_registered_templates()

    for template_name, template_cls in registered.items():
        template = template_cls()

        # 收集该模板的所有可能指标
        metrics_found = set()
        seed_to_metric = {}

        # 扫描 seeds 找到所有指标
        for seed in range(0, 1000):
            try:
                generated = template.generate(seed)
                validation_info = generated.validation_info

                # 提取指标标识
                metric_key = _extract_metric_key(template_name, validation_info)

                if metric_key and metric_key not in metrics_found:
                    metrics_found.add(metric_key)
                    seed_to_metric[seed] = metric_key

                    test_cases.append(TestCase(
                        template_name=template_name,
                        metric=metric_key,
                        seed=seed,
                        question=generated.question_text,
                        expected_type=_get_expected_type(template_name, validation_info),
                    ))
            except Exception as e:
                print(f"Warning: {template_name} seed={seed} error: {e}")
                continue

        print(f"[{template_name}] Found {len(metrics_found)} metrics: {sorted(metrics_found)}")

    return test_cases


def _extract_metric_key(template_name: str, validation_info: dict) -> Optional[str]:
    """从 validation_info 提取指标标识"""
    # 根据不同模板提取不同的指标
    if "metric" in validation_info:
        return validation_info["metric"]
    if "change_type" in validation_info:
        return validation_info["change_type"]
    if "comparison_type" in validation_info:
        return validation_info["comparison_type"]
    if "ranking_by" in validation_info:
        return f"ranking_{validation_info['ranking_by']}"
    if "subnet_id" in validation_info:
        # subnet_info 按指标类型区分
        if "metric" in validation_info:
            return validation_info["metric"]
        return f"subnet_{validation_info.get('metric', 'info')}"

    # 默认使用 template_name
    return "default"


def _get_expected_type(template_name: str, validation_info: dict) -> str:
    """获取期望的答案类型描述"""
    metric = validation_info.get("metric", "")

    type_mapping = {
        # Tokenomics
        "circulating_supply": "数字 (如 10,591,079)",
        "total_supply": "数字 (如 21,000,000)",
        "next_halving": "日期 (如 12 Dec 2029)",
        "in_circulation_pct": "百分比 (如 50.43%)",

        # Validator
        "top_by_stake": "验证者名称 + stake 数量",
        "top_by_nominations": "验证者名称 + nominations 数量",
        "top_by_dominance": "验证者名称 + dominance 百分比",

        # Price change
        "tao_24h": "百分比变化 (如 -2.26%)",
        "subnet_24h": "百分比变化 (如 +1.5%)",

        # Subnet info
        "name": "子网名称",
        "owner": "地址 (5xxx...)",
        "github": "GitHub URL",
        "emission": "emission 百分比",

        # Ranking
        "ranking_market_cap": "排名数字",
        "ranking_emission": "排名数字",
        "ranking_price": "排名数字",
    }

    return type_mapping.get(metric, "具体数值")


def list_test_cases(test_cases: List[TestCase]):
    """列出所有测试用例"""
    print("\n" + "=" * 80)
    print("测试用例总览")
    print("=" * 80)

    # 按模板分组
    by_template = {}
    for tc in test_cases:
        if tc.template_name not in by_template:
            by_template[tc.template_name] = []
        by_template[tc.template_name].append(tc)

    for template_name in sorted(by_template.keys()):
        cases = by_template[template_name]
        print(f"\n### {template_name} ({len(cases)} 指标)")
        print("-" * 60)
        for tc in cases:
            print(f"  [{tc.metric}] seed={tc.seed}")
            print(f"    问题: {tc.question[:70]}...")
            print(f"    期望: {tc.expected_type}")

    print(f"\n总计: {len(test_cases)} 个测试用例")


async def run_single_test(test_case: TestCase, timeout: int = 180) -> TestResult:
    """运行单个测试"""
    import time
    from liveweb_arena.core.browser import BrowserEngine
    from liveweb_arena.core.agent_loop import AgentLoop
    from liveweb_arena.core.agent_policy import AgentPolicy
    from liveweb_arena.utils.llm_client import LLMClient
    import os

    start_time = time.time()

    try:
        # 初始化组件
        browser = BrowserEngine(headless=True)
        await browser.start()

        try:
            session = await browser.new_session()

            try:
                llm_client = LLMClient(
                    base_url=os.getenv("LLM_BASE_URL", "https://llm.chutes.ai/v1"),
                    api_key=os.getenv("CHUTES_API_KEY"),
                )

                agent_loop = AgentLoop(
                    session=session,
                    llm_client=llm_client,
                    policy=AgentPolicy(),
                    max_steps=20,
                )

                # 构建任务
                class SimpleTask:
                    def __init__(self, question):
                        self.combined_intent = f"## Task\n\n{question}\n\nProvide your answer using the stop action."
                        self.plugin_hints = {}
                        self.subtasks = []

                task = SimpleTask(test_case.question)

                # 运行 agent
                trajectory, final_answer, usage = await asyncio.wait_for(
                    agent_loop.run(
                        task=task,
                        model=os.getenv("LLM_MODEL", "zai-org/GLM-4.7-TEE"),
                        temperature=0.7,
                        seed=None,
                    ),
                    timeout=timeout,
                )

                time_taken = time.time() - start_time

                # 解析答案
                actual_answer = None
                if final_answer:
                    if isinstance(final_answer, dict):
                        answers = final_answer.get("answers", final_answer)
                        actual_answer = str(list(answers.values())[0]) if answers else str(final_answer)
                    else:
                        actual_answer = str(final_answer)

                # 简单评估（人工检查更可靠）
                passed = actual_answer is not None and len(actual_answer) > 0
                score = 1.0 if passed else 0.0

                return TestResult(
                    test_case=test_case,
                    passed=passed,
                    actual_answer=actual_answer,
                    score=score,
                    reasoning="Agent returned answer" if passed else "No answer",
                    time_taken=time_taken,
                )

            finally:
                await session.close()

        finally:
            await browser.stop()

    except asyncio.TimeoutError:
        return TestResult(
            test_case=test_case,
            passed=False,
            actual_answer=None,
            score=0.0,
            reasoning="Timeout",
            time_taken=timeout,
            error="Timeout",
        )
    except Exception as e:
        return TestResult(
            test_case=test_case,
            passed=False,
            actual_answer=None,
            score=0.0,
            reasoning=f"Error: {e}",
            time_taken=time.time() - start_time,
            error=str(e),
        )


async def run_tests(
    test_cases: List[TestCase],
    template_filter: Optional[str] = None,
    max_concurrent: int = 1,
) -> List[TestResult]:
    """运行测试"""
    # 过滤
    if template_filter:
        test_cases = [tc for tc in test_cases if template_filter in tc.template_name]

    print(f"\n运行 {len(test_cases)} 个测试...")
    results = []

    for i, tc in enumerate(test_cases):
        print(f"\n[{i+1}/{len(test_cases)}] {tc.template_name}:{tc.metric}")
        print(f"  问题: {tc.question[:60]}...")

        result = await run_single_test(tc)
        results.append(result)

        status = "✓ PASS" if result.passed else "✗ FAIL"
        print(f"  结果: {status} (score={result.score:.2f}, time={result.time_taken:.1f}s)")
        if result.actual_answer:
            print(f"  答案: {result.actual_answer[:60]}...")
        if result.error:
            print(f"  错误: {result.error}")

    return results


def save_results(results: List[TestResult], output_path: Path):
    """保存测试结果"""
    output = {
        "timestamp": datetime.now().isoformat(),
        "total": len(results),
        "passed": sum(1 for r in results if r.passed),
        "failed": sum(1 for r in results if not r.passed),
        "results": [
            {
                "template": r.test_case.template_name,
                "metric": r.test_case.metric,
                "seed": r.test_case.seed,
                "question": r.test_case.question,
                "passed": r.passed,
                "actual_answer": r.actual_answer,
                "score": r.score,
                "reasoning": r.reasoning,
                "time_taken": r.time_taken,
                "error": r.error,
            }
            for r in results
        ],
    }

    with open(output_path, "w") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\n结果已保存到: {output_path}")


def print_summary(results: List[TestResult]):
    """打印测试摘要"""
    print("\n" + "=" * 80)
    print("测试摘要")
    print("=" * 80)

    # 按模板分组
    by_template = {}
    for r in results:
        name = r.test_case.template_name
        if name not in by_template:
            by_template[name] = {"passed": 0, "failed": 0, "results": []}
        by_template[name]["results"].append(r)
        if r.passed:
            by_template[name]["passed"] += 1
        else:
            by_template[name]["failed"] += 1

    for name in sorted(by_template.keys()):
        data = by_template[name]
        total = data["passed"] + data["failed"]
        print(f"\n{name}: {data['passed']}/{total} passed")
        for r in data["results"]:
            status = "✓" if r.passed else "✗"
            print(f"  {status} {r.test_case.metric}")

    total_passed = sum(1 for r in results if r.passed)
    total = len(results)
    print(f"\n总计: {total_passed}/{total} ({100*total_passed/total:.1f}%) 通过")


def generate_questions_only(test_cases: List[TestCase]):
    """只生成问题，不运行"""
    print("\n生成的问题：\n")

    for tc in test_cases:
        print(f"[{tc.template_name}:{tc.metric}] seed={tc.seed}")
        print(f"  {tc.question}")
        print()


async def main():
    parser = argparse.ArgumentParser(description="系统化测试所有模板")
    parser.add_argument("--list", action="store_true", help="列出所有测试用例")
    parser.add_argument("--run", action="store_true", help="运行测试")
    parser.add_argument("--generate", action="store_true", help="只生成问题")
    parser.add_argument("--template", type=str, help="只测试特定模板")
    parser.add_argument("--output", type=str, default="test_results.json", help="输出文件")

    args = parser.parse_args()

    # 发现测试用例
    test_cases = discover_test_cases()

    if args.list:
        list_test_cases(test_cases)
    elif args.generate:
        generate_questions_only(test_cases)
    elif args.run:
        results = await run_tests(test_cases, template_filter=args.template)
        print_summary(results)
        save_results(results, Path(args.output))
    else:
        parser.print_help()


if __name__ == "__main__":
    asyncio.run(main())
