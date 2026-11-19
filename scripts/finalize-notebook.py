"""Validate the executed lakehouse lesson and render its HTML copy."""
import json
from pathlib import Path

import nbformat
from nbconvert import HTMLExporter


root = Path(__file__).resolve().parents[1]
executed = nbformat.read(root/'data_platform_architecture_lab.executed.ipynb', 4)
template = nbformat.read(root/'notebook_template.ipynb', 4)
assert [(cell.id, cell.source) for cell in executed.cells] == [(cell.id, cell.source) for cell in template.cells]
code_cells = [cell for cell in executed.cells if cell.cell_type == 'code']
errors = [output for cell in code_cells for output in cell.outputs if output.output_type == 'error']
assert not errors and all(cell.execution_count is not None for cell in code_cells)
nbformat.validate(executed)
executed.metadata['lab_verification'] = {'engine': 'lakehouse', 'full_restart_run_all_passed': True}
nbformat.write(executed, root/'data_platform_architecture_lab.ipynb')
html, _ = HTMLExporter().from_notebook_node(executed)
(root/'data_platform_architecture_lab.html').write_text(html, encoding='utf-8')
result = {'engine': 'lakehouse', 'nbformat_validation': 'passed', 'executed_code_cells': len(code_cells),
          'errors': len(errors), 'html_export': 'created'}
(root/'artifacts/notebook-validation.json').write_text(json.dumps(result, indent=2))
print(json.dumps(result))
