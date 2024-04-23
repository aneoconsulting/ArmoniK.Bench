import argparse
import pathlib
import subprocess
import toml


def extra_deps_from_pyproject(sections):
    pyproject = pathlib.Path(__file__).parent / "pyproject.toml"

    with pyproject.open() as file:
        data = toml.load(file)

    dependencies = []

    for section in sections:
        try:
            dependencies.extend(data["project"]["optional-dependencies"][section])
        except KeyError:
            raise ValueError(f"Section '{section}' is not a valid optional dependencies section.")

    dependencies.append(
        f"armonik_bench @ git+https://github.com/aneoconsulting/ArmoniK.Bench.git@{subprocess.check_output(['git', 'rev-parse', 'HEAD']).decode('utf-8').strip()}"
    )

    return dependencies


def generate_requirements_file(dependencies):
    requirements = pathlib.Path(__file__).parent / "requirements.txt"

    with requirements.open("w") as file:
        file.write("\n".join(sorted(dependencies)) + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="Extract dependencies from pyproject.toml and generate the corresponding requirements.txt file."
    )
    parser.add_argument(
        "--sections",
        required=True,
        type=str,
        help="Comma-separated string containing the optional dependencies sections to look at.",
    )

    args = parser.parse_args()

    generate_requirements_file(extra_deps_from_pyproject(args.sections.split(",")))


if __name__ == "__main__":
    main()
