# LiveWeb Arena 训练支持设计方案

## 1. 设计目标

将现有的推理/评测框架扩展为支持强化学习训练的环境，遵循 OpenAI Gym 风格接口。

## 2. 接口设计

### 2.1 核心接口 (Gym-like)

```python
class LiveWebEnv:
    """
    OpenAI Gym 风格的 Web 浏览环境
    """

    async def reset(
        self,
        seed: int = None,
        task_config: dict = None
    ) -> Observation:
        """
        重置环境，生成新任务

        Args:
            seed: 随机种子，用于确定性任务生成
            task_config: 可选的任务配置 (plugin, template, num_subtasks等)

        Returns:
            Observation: 初始观察 (about:blank 页面状态 + 任务描述)
        """
        pass

    async def step(self, action: Action) -> StepResult:
        """
        执行一个动作

        Args:
            action: 要执行的浏览器动作

        Returns:
            StepResult: (observation, reward, done, truncated, info)
        """
        pass

    @property
    def state(self) -> EnvState:
        """
        获取当前环境状态（可用于保存/恢复）
        """
        pass

    def close(self):
        """
        关闭环境，释放资源
        """
        pass
```

### 2.2 数据结构

```python
@dataclass
class Observation:
    """环境观察"""
    url: str                      # 当前 URL
    title: str                    # 页面标题
    accessibility_tree: str       # 可访问性树（截断）
    task_intent: str              # 任务描述
    step_num: int                 # 当前步数

@dataclass
class Action:
    """浏览器动作"""
    action_type: str              # goto|click|type|scroll|press|wait|stop|click_role|type_role
    params: dict                  # 动作参数

@dataclass
class StepResult:
    """单步执行结果"""
    observation: Observation      # 新的观察
    reward: float                 # 即时奖励
    done: bool                    # 是否结束（成功完成或失败）
    truncated: bool               # 是否截断（超过最大步数）
    info: dict                    # 额外信息

@dataclass
class EnvState:
    """可序列化的环境状态"""
    task: CompositeTask           # 当前任务
    trajectory: List[TrajectoryStep]  # 历史轨迹
    step_num: int                 # 当前步数
    # 注：浏览器状态通过 URL 恢复，不直接序列化
```

## 3. 奖励函数设计

### 3.1 奖励组成

```
total_reward = final_reward + step_reward + shaping_reward
```

| 奖励类型 | 时机 | 范围 | 说明 |
|---------|------|------|------|
| **final_reward** | episode 结束 | [0, 1] | 基于答案正确性的最终奖励 |
| **step_reward** | 每一步 | [-0.01, 0] | 步数惩罚，鼓励高效 |
| **shaping_reward** | 每一步 | [-0.1, 0.1] | 进度塑形奖励（可选） |

### 3.2 Final Reward（最终奖励）

```python
def compute_final_reward(validation_result: ValidationResult) -> float:
    """
    基于答案验证结果计算最终奖励

    validation_result.score: 0.0 | 0.5 | 1.0 (来自 LLM Validator)
    """
    return validation_result.score  # 直接使用验证分数
```

### 3.3 Step Reward（步数奖励）

```python
def compute_step_reward(step_num: int, max_steps: int) -> float:
    """
    每一步的小惩罚，鼓励更快完成任务
    """
    return -0.01  # 固定小惩罚

    # 或递增惩罚（越拖延惩罚越大）
    # return -0.01 * (1 + step_num / max_steps)
```

### 3.4 Shaping Reward（塑形奖励，可选）

```python
def compute_shaping_reward(
    prev_obs: Observation,
    curr_obs: Observation,
    action: Action,
    task: CompositeTask
) -> float:
    """
    基于进度的塑形奖励（可选，可能引入 reward hacking）

    可选的塑形信号：
    1. URL 变化：从 about:blank 到目标站点 → +0.05
    2. 页面加载成功：action 执行无错误 → +0.01
    3. 找到相关内容：页面包含任务关键词 → +0.02
    """
    reward = 0.0

    # 示例：导航到目标站点
    if prev_obs.url == "about:blank" and task.target_site in curr_obs.url:
        reward += 0.05

    return reward
```

### 3.5 奖励配置

```python
@dataclass
class RewardConfig:
    """奖励函数配置"""
    # 最终奖励
    use_final_reward: bool = True
    final_reward_scale: float = 1.0

    # 步数奖励
    use_step_penalty: bool = True
    step_penalty: float = -0.01

    # 塑形奖励
    use_shaping: bool = False  # 默认关闭，避免 reward hacking
    shaping_scale: float = 0.1

    # 失败惩罚
    failure_penalty: float = -0.5  # 超时或执行错误
```

## 4. Episode 终止条件

```python
def check_done(action: Action, step_num: int, max_steps: int, error: Exception) -> Tuple[bool, bool]:
    """
    检查 episode 是否结束

    Returns:
        (done, truncated)
        - done=True, truncated=False: 正常结束（agent 发出 stop）
        - done=True, truncated=True: 截断（超过 max_steps）
        - done=True, truncated=True: 错误终止
    """
    if action.action_type == "stop":
        return True, False  # 正常结束

    if step_num >= max_steps:
        return True, True  # 截断

    if error is not None:
        return True, True  # 错误终止

    return False, False  # 继续
```

## 5. 实现架构

### 5.1 类图

```
┌─────────────────────────────────────────────────────────────────┐
│                        LiveWebEnv                                │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐ │
│  │ TaskManager │  │BrowserEngine│  │    RewardCalculator     │ │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘ │
│         │                │                      │               │
│         ▼                ▼                      ▼               │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐ │
│  │CompositeTask│  │BrowserSession│ │    LLMValidator         │ │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘

复用现有组件：
- TaskManager: 任务生成
- BrowserEngine/Session: 浏览器控制
- LLMValidator: 答案验证
- Plugin 系统: 任务模板
```

### 5.2 文件结构

```
liveweb_arena/
├── core/
│   ├── ... (现有文件)
│   └── env/                    # 新增：训练环境
│       ├── __init__.py
│       ├── base.py             # LiveWebEnv 基类
│       ├── observation.py      # Observation, Action 数据类
│       ├── reward.py           # RewardCalculator, RewardConfig
│       └── wrappers.py         # 环境包装器（标准化、记录等）
```

### 5.3 核心实现

```python
# liveweb_arena/core/env/base.py

class LiveWebEnv:
    def __init__(
        self,
        plugins: List[str] = None,
        max_steps: int = 30,
        reward_config: RewardConfig = None,
        browser_config: dict = None,
        llm_config: dict = None,  # 用于验证
    ):
        self._plugins = plugins or ["taostats", "weather"]
        self._max_steps = max_steps
        self._reward_config = reward_config or RewardConfig()
        self._browser_config = browser_config or {}
        self._llm_config = llm_config or {}

        # 组件（懒加载）
        self._task_manager = None
        self._browser = None
        self._session = None
        self._validator = None

        # 状态
        self._task = None
        self._trajectory = []
        self._step_num = 0
        self._done = False

    async def reset(self, seed: int = None, task_config: dict = None) -> Observation:
        # 1. 清理旧 session
        if self._session:
            await self._session.close()

        # 2. 确保组件初始化
        await self._ensure_components()

        # 3. 生成任务
        self._task = await self._task_manager.generate_composite_task(
            seed=seed or random.randint(0, 2**31),
            num_subtasks=task_config.get("num_subtasks", 1) if task_config else 1,
            plugin_names=task_config.get("plugins") if task_config else None,
        )

        # 4. 创建新 session
        self._session = await self._browser.new_session()

        # 5. 重置状态
        self._trajectory = []
        self._step_num = 0
        self._done = False

        # 6. 获取初始观察
        browser_obs = await self._session._get_observation()
        return self._make_observation(browser_obs)

    async def step(self, action: Action) -> StepResult:
        if self._done:
            raise RuntimeError("Episode已结束，请调用reset()")

        # 1. 执行动作
        prev_obs = await self._session._get_observation()
        try:
            browser_action = BrowserAction(action.action_type, action.params)
            result = await self._session.execute_action(browser_action)
            error = None
        except Exception as e:
            result = f"Error: {e}"
            error = e

        # 2. 获取新观察
        curr_browser_obs = await self._session._get_observation()
        curr_obs = self._make_observation(curr_browser_obs)

        # 3. 记录轨迹
        self._trajectory.append(TrajectoryStep(
            step_num=self._step_num,
            observation=prev_obs,
            action=browser_action,
            action_result=result,
        ))
        self._step_num += 1

        # 4. 检查终止
        done, truncated = self._check_done(action, error)
        self._done = done

        # 5. 计算奖励
        reward = await self._compute_reward(
            action=action,
            prev_obs=prev_obs,
            curr_obs=curr_browser_obs,
            done=done,
            truncated=truncated,
            error=error,
        )

        # 6. 构建 info
        info = {
            "action_result": result,
            "error": str(error) if error else None,
        }
        if done and action.action_type == "stop":
            # 解析答案并验证
            answers = self._parse_answers(action.params.get("final", {}))
            validation = await self._validate_answers(answers)
            info["validation"] = validation
            info["answers"] = answers

        return StepResult(
            observation=curr_obs,
            reward=reward,
            done=done,
            truncated=truncated,
            info=info,
        )

    async def _compute_reward(self, action, prev_obs, curr_obs, done, truncated, error) -> float:
        reward = 0.0
        cfg = self._reward_config

        # 步数惩罚
        if cfg.use_step_penalty:
            reward += cfg.step_penalty

        # 错误惩罚
        if error:
            reward += cfg.failure_penalty

        # 塑形奖励
        if cfg.use_shaping:
            reward += self._compute_shaping(prev_obs, curr_obs, action) * cfg.shaping_scale

        # 最终奖励
        if done and not truncated and action.action_type == "stop":
            if cfg.use_final_reward:
                answers = self._parse_answers(action.params.get("final", {}))
                validation = await self._validate_answers(answers)
                final_score = sum(v["score"] for v in validation) / len(validation)
                reward += final_score * cfg.final_reward_scale
        elif truncated:
            reward += cfg.failure_penalty  # 超时惩罚

        return reward
```

## 6. 使用示例

### 6.1 基本训练循环

```python
import asyncio
from liveweb_arena.core.env import LiveWebEnv, RewardConfig

async def train_episode(env, policy):
    """单个 episode 的训练"""
    obs = await env.reset(seed=12345)

    total_reward = 0
    while True:
        # 策略选择动作
        action = policy.select_action(obs)

        # 执行动作
        result = await env.step(action)

        # 记录经验
        policy.store_transition(obs, action, result.reward, result.observation, result.done)

        total_reward += result.reward
        obs = result.observation

        if result.done:
            break

    return total_reward

async def main():
    # 创建环境
    env = LiveWebEnv(
        plugins=["taostats"],
        max_steps=30,
        reward_config=RewardConfig(
            use_final_reward=True,
            use_step_penalty=True,
            step_penalty=-0.01,
        ),
    )

    # 创建策略（例如 PPO）
    policy = YourRLPolicy()

    # 训练循环
    for episode in range(1000):
        reward = await train_episode(env, policy)
        print(f"Episode {episode}: reward={reward:.2f}")

        # 更新策略
        policy.update()

    env.close()

asyncio.run(main())
```

### 6.2 并行环境（向量化）

```python
from liveweb_arena.core.env import VectorLiveWebEnv

async def main():
    # 创建 4 个并行环境
    vec_env = VectorLiveWebEnv(
        num_envs=4,
        plugins=["taostats"],
        max_steps=30,
    )

    # 批量 reset
    obs_batch = await vec_env.reset(seeds=[100, 200, 300, 400])

    # 批量 step
    actions = [policy.select_action(obs) for obs in obs_batch]
    results = await vec_env.step(actions)

    vec_env.close()
```

## 7. 奖励函数的权衡

### 7.1 稀疏奖励 vs 稠密奖励

| 方案 | 优点 | 缺点 |
|------|------|------|
| **纯稀疏** (只有 final_reward) | 奖励信号干净，无 reward hacking | 训练困难，需要更多探索 |
| **稠密** (final + step + shaping) | 训练更稳定，收敛更快 | 可能导致 reward hacking |

**建议**：从纯稀疏开始，如果训练困难再逐步添加 shaping。

### 7.2 Shaping Reward 的风险

塑形奖励可能引导 agent 学到错误的行为：
- 奖励"找到关键词"→ agent 可能学会随便翻页而不真正理解内容
- 奖励"导航到目标站点"→ agent 可能直接 stop 而不完成任务

**缓解措施**：
1. 保持 shaping 奖励远小于 final 奖励 (例如 0.1x)
2. 使用 potential-based shaping（理论上等价于原问题）
3. 定期评估策略在无 shaping 环境中的表现

## 8. 与现有代码的集成

### 8.1 复用的组件

| 组件 | 用途 | 修改 |
|------|------|------|
| `TaskManager` | 任务生成 | 无需修改 |
| `BrowserEngine` | 浏览器管理 | 无需修改 |
| `BrowserSession` | 页面交互 | 无需修改 |
| `Plugin 系统` | 任务模板 | 无需修改 |
| `LLMValidator` | 答案验证 | 无需修改 |
| `AnswerParser` | 答案解析 | 无需修改 |

### 8.2 新增的组件

| 组件 | 职责 |
|------|------|
| `LiveWebEnv` | Gym 接口封装 |
| `RewardCalculator` | 奖励计算 |
| `VectorLiveWebEnv` | 并行环境 |
| `EnvWrapper` | 观察/动作标准化 |

## 9. 实现优先级

### Phase 1: 基础环境 (必须)
- [ ] `LiveWebEnv` 基类实现
- [ ] `reset()` / `step()` 接口
- [ ] 基本奖励函数 (final + step penalty)
- [ ] Episode 终止逻辑

### Phase 2: 奖励增强 (可选)
- [ ] Shaping reward 实现
- [ ] 奖励配置系统
- [ ] 奖励日志记录

### Phase 3: 扩展功能 (可选)
- [ ] `VectorLiveWebEnv` 并行环境
- [ ] 环境包装器 (标准化、记录)
- [ ] 状态序列化/恢复

## 10. 待确认问题

1. **奖励时机**：最终奖励是在 `stop` 动作时立即计算，还是需要异步验证后再给？
   - 当前设计：在 `step()` 中同步计算（可能有延迟）

2. **验证成本**：每次 episode 结束都需要调用 LLM 验证，是否接受这个延迟？
   - 可选：使用规则验证代替 LLM 验证（更快但不够灵活）

3. **浏览器状态恢复**：`state` 属性是否需要支持完整的浏览器状态恢复？
   - 当前设计：只保存 URL，通过 `goto` 恢复（可能丢失页面状态）

4. **并行度**：训练时需要多少并行环境？
   - 影响浏览器资源消耗

5. **动作空间表示**：是否需要将动作空间离散化/标准化？
   - 当前：自由格式的 JSON 动作
   - 可选：离散化为有限动作集
