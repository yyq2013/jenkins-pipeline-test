package com.test.mytask.service.conveyorPaperPick;

import com.test.common.service.client.user.bo.UserInfo;
import com.test.mytask.service.bo.conveyorPaperPick.AlterBinResponse;
import com.test.mytask.service.bo.conveyorPaperPick.BoxSize;
import com.test.mytask.service.bo.conveyorPaperPick.BoxValidationRequest;
import com.test.mytask.service.bo.conveyorPaperPick.ConveyorMultiPaperlessDoPickRequest;
import com.test.mytask.service.bo.conveyorPaperPick.PickToToteInnerRequest;

import java.util.List;
import java.util.Map;

public interface ConveyorPaperPickService {
    boolean inputPickToTote(PickToToteInnerRequest request);

    boolean checkCartonNoWhetherUsed(Integer locNo, String cartonNo);

    List<AlterBinResponse> showAvailableBin(Integer locNo, String zone, Integer skuNo, Integer invType);

    boolean newTote(String zone, Integer locNo, String curToteId, String newToteId, Boolean bypassQC);

    BoxSize validateBoxId(BoxValidationRequest request, UserInfo userInfo);

    Map<String, Object> doPick(ConveyorMultiPaperlessDoPickRequest request, UserInfo userInfo);
}
