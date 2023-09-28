import subprocess
from timeit import default_timer as timer
import git
import typer
from rich import print


def analyse_performance_history(
    rev: str, limit_commits: int, sample_amount: int, print_all_results: bool
):
    limit = limit_commits
    repo = git.Repo()

    for commit in repo.iter_commits(rev=rev):
        if limit == 0:
            break
        limit -= 1

        repo.git.checkout(commit)
        head = repo.head.commit

        results = []
        for _ in range(sample_amount):
            start = timer()
            subprocess.run(["snow", "--help"], stdout=subprocess.PIPE)
            end = timer()
            results.append(end - start)

        results.sort()
        result = results[int(sample_amount * 0.9)]

        print(
            f"Result [{result}] for commit {head} ({head.authored_datetime}) -> {head.message.splitlines()[0]}"
        )
        if print_all_results:
            print(results.__str__() + "\n")


def main(
    rev: str = "HEAD",
    limit_commits: int = 50,
    sample_amount: int = 20,
    print_all_results: bool = True,
):
    analyse_performance_history(
        rev=rev,
        limit_commits=limit_commits,
        sample_amount=sample_amount,
        print_all_results=print_all_results,
    )


if __name__ == "__main__":
    typer.run(main)
