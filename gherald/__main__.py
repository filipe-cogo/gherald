# type: ignore[attr-defined]
from typing import Optional

from enum import Enum
from random import choice

import typer
from rich.console import Console

from gherald import version


class Color(str, Enum):
    white = "white"
    red = "red"
    cyan = "cyan"
    magenta = "magenta"
    yellow = "yellow"
    green = "green"


app = typer.Typer(
    name="gherald",
    help="A python package to assess the risk of a change during code review.",
    add_completion=False,
)
console = Console()


def version_callback(print_version: bool) -> None:
    if print_version:
        console.print(f"[yellow]gherald[/] version: [bold blue]{version}[/]")
        raise typer.Exit()


@app.command(name="")
def main(
    name: str = typer.Option(..., help="Person to greet."),
    color: Optional[Color] = typer.Option(
        None,
        "-c",
        "--color",
        "--colour",
        case_sensitive=False,
        help="Color for print. If not specified then choice will be random.",
    ),
    print_version: bool = typer.Option(
        None,
        "-v",
        "--version",
        callback=version_callback,
        is_eager=True,
        help="Prints the version of the gherald package.",
    ),
) -> None:
    if color is None:
        color = choice(list(Color))

    greeting: str = "Hello"
    console.print(f"[bold {color}]{greeting}[/]")


if __name__ == "__main__":
    app()
