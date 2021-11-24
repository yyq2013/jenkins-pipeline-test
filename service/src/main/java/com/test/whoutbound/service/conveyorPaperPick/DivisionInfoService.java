package com.test.mytask.service.conveyorPaperPick;

import com.synnex.common.service.user.bo.DivisionInfo;

import java.util.List;
import java.util.Map;

/**
 * Created by richardf on 2017/8/7.
 */
public interface DivisionInfoService {

	DivisionInfo getDivisionInfoByDivNoUserCIS(Integer division);

	List<DivisionInfo> getDivisionInfoByDivNoAndSourceType(Integer division, List<String> sourceTypes);

	Map<Integer, DivisionInfo> getAllDivisionInfosUseCIS();

	List<DivisionInfo> getAllDivisionInfosAndSourceType(List<String> sourceTypes);
}
