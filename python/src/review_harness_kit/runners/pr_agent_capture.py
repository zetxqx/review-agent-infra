from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import yaml


async def run_pr_reviewer(target_branch: str, out_path: str) -> None:
    from pr_agent.tools.pr_reviewer import PRReviewer

    reviewer = PRReviewer(target_branch)
    await reviewer.run()
    prediction_data = getattr(reviewer, "prediction_data", None)
    if prediction_data is not None:
        text = yaml.safe_dump(prediction_data, sort_keys=False)
    else:
        text = getattr(reviewer, "prediction", "") or ""
    target = Path(out_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")


def main() -> None:
    asyncio.run(run_pr_reviewer(sys.argv[1], sys.argv[2]))


if __name__ == "__main__":
    main()
