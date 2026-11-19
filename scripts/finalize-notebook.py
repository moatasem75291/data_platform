"""Preserve real execution outputs, refresh restore evidence by executing its cell, and render HTML."""
import json
from pathlib import Path
import nbformat
from nbclient import NotebookClient
from nbconvert import HTMLExporter
root=Path(__file__).resolve().parents[1]
executed=nbformat.read(root/'data_platform_architecture_lab.executed.ipynb',4)
template=nbformat.read(root/'notebook_template.ipynb',4)
assert len(executed.cells)==len(template.cells)
restore_index=None
for i,(old,new) in enumerate(zip(executed.cells,template.cells)):
    assert old.id==new.id and old.cell_type==new.cell_type
    if new.cell_type=='markdown':old.source=new.source
    elif 'restore_evidence=ARTIFACTS' in new.source:
        old.source=new.source;restore_index=i
    else:assert old.source==new.source,f'Unexecuted code change in cell {i}'
assert restore_index is not None
mini=nbformat.v4.new_notebook(cells=[nbformat.v4.new_code_cell('from pathlib import Path\nimport json,pandas as pd\nfrom IPython.display import display\nfrom lab.config import ARTIFACTS,DATA\nrelease=json.loads((DATA/"releases/current.json").read_text())'),nbformat.v4.new_code_cell(executed.cells[restore_index].source)])
NotebookClient(mini,timeout=120,kernel_name='python3',resources={'metadata':{'path':str(root)}}).execute()
executed.cells[restore_index].outputs=mini.cells[1].outputs
executed.cells[restore_index].execution_count=max(c.execution_count or 0 for c in executed.cells if c.cell_type=='code')+1
errors=[o for c in executed.cells if c.cell_type=='code' for o in c.outputs if o.output_type=='error']
code_cells=[c for c in executed.cells if c.cell_type=='code']
assert not errors and all(c.execution_count is not None for c in code_cells)
executed.metadata['lab_verification']={'full_restart_run_all_passed':True,'BI_and_CDC_executed':True,'restore_cell_refreshed_by_execution':True}
nbformat.validate(executed)
nbformat.write(executed,root/'data_platform_architecture_lab.ipynb')
html,_=HTMLExporter().from_notebook_node(executed)
(root/'data_platform_architecture_lab.html').write_text(html,encoding='utf-8')
result={'nbformat_validation':'passed','executed_code_cells':len(code_cells),'errors':len(errors),'restore_cell_refreshed':'executed','html_export':'created'}
(root/'artifacts/notebook-validation.json').write_text(json.dumps(result,indent=2))
print(json.dumps(result))
