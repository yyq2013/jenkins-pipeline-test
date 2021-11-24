package com.test.mytask.service.conveyorPaperPick;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.synnex.base.service.util.ObjectUtil;
import com.synnex.common.service.cache.CacheDataService;
import com.synnex.common.service.dao.masterData.userinfo.entity.DivisionInfo;
import com.synnex.common.service.dao.masterData.userinfo.entity.Manager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by richardf on 2017/8/7.
 */
@Service
public class DivisionInfoServiceImpl implements DivisionInfoService {
	@Autowired
	CacheDataService cacheDataService;


	@Override
	public com.synnex.common.service.user.bo.DivisionInfo getDivisionInfoByDivNoUserCIS(Integer division) {
		List<String> sourctTypes = new ArrayList<>();
		sourctTypes.add("CIS");
		List<com.synnex.common.service.user.bo.DivisionInfo> divisionInfos = getDivisionInfoByDivNoAndSourceType(division,sourctTypes);
		if(!ObjectUtil.isEmpty(divisionInfos)){
			return divisionInfos.get(0);
		}
		return null;
	}

	@Override
	public List<com.synnex.common.service.user.bo.DivisionInfo> getDivisionInfoByDivNoAndSourceType(Integer division, List<String> sourceTypes) {
		List<DivisionInfo> infos = cacheDataService.getDivisionInfo(division);
		List<com.synnex.common.service.user.bo.DivisionInfo> rstValues = new ArrayList<>();
		if (!ObjectUtil.isEmpty(infos)) {
			for(DivisionInfo divisionInfo:infos) {
				if(matchSourceTypes(sourceTypes, divisionInfo.getId().getSourceType())) {
					Manager manager = cacheDataService.getManagerById(divisionInfo.getVpId());
					String vpUserName=null;
					if(manager!=null) {
						 vpUserName=manager.getFirstname()+" "+manager.getLastname();
					}
					com.synnex.common.service.user.bo.DivisionInfo divisionInfoBo=getDivision(divisionInfo);
					divisionInfoBo.setVpUserName(vpUserName);
					rstValues.add(divisionInfoBo);
				}
			}
		}
		return rstValues;
	}

	@Override
	public Map<Integer, com.synnex.common.service.user.bo.DivisionInfo> getAllDivisionInfosUseCIS() {
		Map<Integer, DivisionInfo> temp = cacheDataService.getDivisionInfoUseCIS();
		Manager manager=null;
		String vpUserName=null;;
		com.synnex.common.service.user.bo.DivisionInfo divisionInfo=null;
		Map<Integer, com.synnex.common.service.user.bo.DivisionInfo> result=new HashMap<>();
		Integer key=null;
		if (!ObjectUtil.isEmpty(temp)) {
			Iterator<Integer> iterator = temp.keySet().iterator();
			while(iterator.hasNext()) {
				key=iterator.next();
				divisionInfo=getDivision(temp.get(key));
				manager = cacheDataService.getManagerById(divisionInfo.getVpId());
				if(manager!=null) {
					vpUserName=manager.getFirstname()+" "+manager.getLastname();
					divisionInfo.setVpUserName(vpUserName);
				}
				    result.put(key, divisionInfo);

			}
			return result;
		}
		return null;
	}

	private com.synnex.common.service.user.bo.DivisionInfo getDivision(DivisionInfo info) {
		com.synnex.common.service.user.bo.DivisionInfo div = new com.synnex.common.service.user.bo.DivisionInfo();
		div.setDivisionNo(info.getId().getDivisionNo());
		div.setCompanyNo(info.getCompanyNo());
		div.setDivisionName(info.getDivisionName());
		div.setVpId(info.getVpId());
		div.setInactive(info.getInactive());
		div.setDefDept(info.getDefDept());
		div.setDefLoc(info.getDefLoc());
		div.setSourceType(info.getId().getSourceType());
		return div;
	}
	private boolean matchSourceTypes(List<String> sourceTypes, String glSourceType) {
		boolean retValue = false;
		if (!ObjectUtil.isEmpty(sourceTypes)) {
			for (String sourceType : sourceTypes) {
				if (glSourceType.equalsIgnoreCase(sourceType)) {
					retValue = true;
					break;
				}
			}
		} else {
			retValue = true;
		}
		return retValue;
	}

	@Override
	public List<com.synnex.common.service.user.bo.DivisionInfo> getAllDivisionInfosAndSourceType(List<String> sourceTypes) {
		// TODO Auto-generated method stub
		List<DivisionInfo> temp = cacheDataService.getDivisionInfoList();
		Manager manager=null;
		String vpUserName=null;;
		List<com.synnex.common.service.user.bo.DivisionInfo> result=new ArrayList<>();
		Integer key=null;
		if (!ObjectUtil.isEmpty(temp)) {
			for(DivisionInfo divisionInfo1:temp) {
				com.synnex.common.service.user.bo.DivisionInfo divisionInfo = getDivision(divisionInfo1);
				manager = cacheDataService.getManagerById(divisionInfo.getVpId());
				if (manager != null) {
					vpUserName = manager.getFirstname() + " " + manager.getLastname();
					divisionInfo.setVpUserName(vpUserName);
				}
				if (matchSourceTypes(sourceTypes, divisionInfo.getSourceType())) {
					result.add(divisionInfo);
				}
			}
			return result;
		}
		return null;
	}
}
