import subprocess
import sys


def run_command(command):
    run_result = subprocess.run(command, shell=True, check=True)
    if run_result.returncode != 0:
        print(f"Command '{command}' failed with return code {run_result.returncode}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python format_code.py <file_path>")
        return

    file_path = sys.argv[1]

    context = {"file_path": file_path}
    commands = [
        "poetry run black {file_path}",
        "poetry run isort {file_path}",
        "poetry run add-trailing-comma {file_path}",
    ]

    for command in commands:
        try:
            run_command(command.format(**context))
        except subprocess.CalledProcessError as error:
            print("Error: {error}".format(error=error))


if __name__ == "__main__":
    main()
