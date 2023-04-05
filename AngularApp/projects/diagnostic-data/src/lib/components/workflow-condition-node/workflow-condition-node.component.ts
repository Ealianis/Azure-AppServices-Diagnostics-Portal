import { Component, OnInit } from '@angular/core';
import { DetectorControlService } from '../../services/detector-control.service';
import { DiagnosticService } from '../../services/diagnostic.service';
import { WorkflowHelperService } from '../../services/workflow-helper.service';
import { WorkflowNodeComponent } from '../workflow-node/workflow-node.component';

@Component({
  selector: 'workflow-condition-node',
  templateUrl: './workflow-condition-node.component.html',
  styleUrls: ['./workflow-condition-node.component.scss']
})
export class WorkflowConditionNodeComponent extends WorkflowNodeComponent implements OnInit {

  constructor(private _diagnosticServicePrivate: DiagnosticService, private _detectorControlServicePrivate: DetectorControlService,
    private _workflowHelperServicePrivate: WorkflowHelperService) {
    super(_diagnosticServicePrivate, _detectorControlServicePrivate, _workflowHelperServicePrivate);
  }

  ngOnInit(): void {
    super.ngOnInit();
  }

}
