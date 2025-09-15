# Quick Python Script Template

## Up and Running

Prerequisites:
1. Python installed
2. AWS creds updated
3. Install `uv` with instructions from `https://docs.astral.sh/uv/getting-started/installation/`

Steps:
1. Open folder in vscode
2. In terminal run the following:
   ```
   uv venv
   source .venv/bin/activate
   uv sync
   ```
3. in vscode `Shift+Cmd+P -> Python:Select Interpreter` -> select the option with `./.venv/bin/python`
4. in vscode, open any script file (like bcm-sync.py) and `Start Debugging` (menu or F5)
