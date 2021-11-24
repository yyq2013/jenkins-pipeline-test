package com.test.mytask.service.conveyorPaperPick;

import com.test.base.service.exception.BizException;
import com.test.base.service.util.ObjectUtil;
import com.test.base.service.util.TransactionMangers;
import com.test.common.service.client.bo.JsonEntity;
import com.test.common.service.client.customer.CustomerServiceClient;
import com.test.common.service.client.customer.bo.Customer;
import com.test.common.service.client.order.OrderServiceClient;
import com.test.common.service.client.order.bo.Order;
import com.test.common.service.client.order.bo.OrderDetail;
import com.test.common.service.client.order.bo.OrderHeader;
import com.test.common.service.client.order.bo.OrderInfoParam;
import com.test.common.service.client.order.bo.OrderParamBase;
import com.test.common.service.client.order.bo.OrderProfile;
import com.test.common.service.client.order.bo.OrderSoldTo;
import com.test.common.service.client.part.PartServiceClient;
import com.test.common.service.client.part.bo.PartMaster;
import com.test.common.service.client.user.bo.UserInfo;
import com.test.mytask.service.ConveyorZone9Sort.ConveyorZone9Sort;
import com.test.mytask.service.bo.common.Box;
import com.test.mytask.service.bo.conveyorPaperPick.AlterBinResponse;
import com.test.mytask.service.bo.conveyorPaperPick.BoxSize;
import com.test.mytask.service.bo.conveyorPaperPick.BoxValidationRequest;
import com.test.mytask.service.bo.conveyorPaperPick.ConveyorMultiPaperlessDoPickRequest;
import com.test.mytask.service.bo.conveyorPaperPick.PickToToteInnerRequest;
import com.test.mytask.service.bo.conveyorPaperPick.PickToToteRequest;
import com.test.mytask.service.bo.conveyorPaperPick.TotePickDetail;
import com.test.mytask.service.bo.conveyorPaperPick.TotePickHeader;
import com.test.mytask.service.bo.paperlessPick.CwsSmartpickStatus;
import com.test.mytask.service.bo.paperlessPick.OrderKey;
import com.test.mytask.service.common.BinMgmtLog;
import com.test.mytask.service.common.CommonService;
import com.test.mytask.service.common.CwsBinMngLogService;
import com.test.mytask.service.common.CwsEventLogEventType;
import com.test.mytask.service.common.CwsPickProgressService;
import com.test.mytask.service.common.OrderService;
import com.test.mytask.service.conveyorPick.ConveyorPickCommonService;
import com.test.mytask.service.conveyorPickInject.ConveyorPickInjectService;
import com.test.mytask.service.conveyorPutawayInject.ConveyorPutawayInjectService;
import com.test.mytask.service.dao.entity.CartonDetail;
import com.test.mytask.service.dao.entity.CartonDetailId;
import com.test.mytask.service.dao.entity.CartonHeader;
import com.test.mytask.service.dao.entity.ConveyorAutoShipQueue;
import com.test.mytask.service.dao.entity.ConveyorAutoShipQueueId;
import com.test.mytask.service.dao.entity.CwsCartonProf;
import com.test.mytask.service.dao.entity.CwsConveyorSocket;
import com.test.mytask.service.dao.entity.CwsEventLog;
import com.test.mytask.service.dao.entity.CwsGenericProf;
import com.test.mytask.service.dao.entity.CwsInv;
import com.test.mytask.service.dao.entity.CwsInvId;
import com.test.mytask.service.dao.entity.CwsPart;
import com.test.mytask.service.dao.entity.CwsPickProgress;
import com.test.mytask.service.dao.entity.CwsSmartpickDetail;
import com.test.mytask.service.dao.entity.CwsSmartpickDetailId;
import com.test.mytask.service.dao.entity.CwsSmartpickHeader;
import com.test.mytask.service.dao.entity.CwsSmartpickHeaderId;
import com.test.mytask.service.dao.entity.SerNoProf;
import com.test.mytask.service.dao.entity.SerialNbr;
import com.test.mytask.service.dao.entity.SerialNbrId;
import com.test.mytask.service.dao.entity.WhProfile;
import com.test.mytask.service.dao.repository.CartonDetailRepository;
import com.test.mytask.service.dao.repository.CartonHeaderRepository;
import com.test.mytask.service.dao.repository.CartonInPackingRepository;
import com.test.mytask.service.dao.repository.ConveyorAutoShipQueueRepository;
import com.test.mytask.service.dao.repository.CwsBinMngLogRepository;
import com.test.mytask.service.dao.repository.CwsCartonProfRepository;
import com.test.mytask.service.dao.repository.CwsConveyorPickHistoryRepository;
import com.test.mytask.service.dao.repository.CwsConveyorPickRepository;
import com.test.mytask.service.dao.repository.CwsConveyorSocketRepository;
import com.test.mytask.service.dao.repository.CwsEventLogRepository;
import com.test.mytask.service.dao.repository.CwsGenericProfRepository;
import com.test.mytask.service.dao.repository.CwsInvRepository;
import com.test.mytask.service.dao.repository.CwsPartRepository;
import com.test.mytask.service.dao.repository.CwsSmartpickDetailRepository;
import com.test.mytask.service.dao.repository.CwsSmartpickHeaderRepository;
import com.test.mytask.service.dao.repository.SerNoProfRepository;
import com.test.mytask.service.dao.repository.SerialNbrRepository;
import com.test.mytask.service.dao.repository.ShipMethodMapRepository;
import com.test.mytask.service.dao.repository.SkuProfileRepository;
import com.test.mytask.service.dao.repository.WhProfileRepository;
import com.test.mytask.service.util.CwsConstants;
import com.test.mytask.service.util.CwsNumberUtil;
import com.test.mytask.service.util.CwsStringUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.CallableStatementCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class ConveyorPaperPickServiceImpl implements ConveyorPaperPickService {

    private static final Logger LOG = LoggerFactory.getLogger(ConveyorPaperPickServiceImpl.class);

    @Autowired
    private ConveyorZone9Sort conveyorZone9Sort;

    @Autowired
    private ConveyorPaperPickValidate conveyorPaperPickValidate;

    @Autowired
    private CwsEventLogRepository cwsEventLogRepository;

    @Autowired
    private CommonService commonService;

    @Autowired
    private ConveyorPutawayInjectService conveyorPutawayInjectService;

    @Autowired
    private SkuProfileRepository skuProfileRepository;

    @Autowired
    private CwsCartonProfRepository cwsCartonProfRepository;

    @Autowired
    private CartonDetailRepository cartonDetailRepository;

    @Autowired
    private SerNoProfRepository serNoProfRepository;

    @Autowired
    private CwsPartRepository cwsPartRepository;

    @Autowired
    private ConveyorPickInjectService conveyorPickInjectService;

    @Autowired
    private OrderService orderService;

    @Autowired
    private ShipMethodMapRepository shipMethodMapRepository;

    @Autowired
    private CwsPickProgressService cwsPickProgressService;

    @Autowired
    private CwsBinMngLogRepository cwsBinMngLogRepository;

    @Autowired
    private CwsConveyorSocketRepository cwsConveyorSocketRepository;

    @Autowired
    @Qualifier(value = "defaultJdbcTemplate")
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private CwsGenericProfRepository cwsGenericProfRepository;

    @Autowired
    private ConveyorAutoShipQueueRepository conveyorAutoShipQueueRepository;

    @Autowired
    private CartonHeaderRepository cartonHeaderRepository;

    @Autowired
    private WhProfileRepository whProfileRepository;

    @Autowired
    private CwsConveyorPickRepository cwsConveyorPickRepository;

    @Autowired
    private CwsConveyorPickHistoryRepository cwsConveyorPickHistoryRepository;

    @Autowired
    private CartonInPackingRepository cartonInPackingRepository;

    @Autowired
    private CwsInvRepository cwsInvRepository;

    @Autowired
    private CwsBinMngLogService cwsBinMngLogService;

    @Autowired
    private SerialNbrRepository serialNbrRepository;

    @Autowired(required = false)
    private PartServiceClient partServiceClient;

    @Autowired(required = false)
    private OrderServiceClient orderServiceClient;

    @Autowired(required = false)
    private CustomerServiceClient customerServiceClient;

    @Autowired
    private CwsSmartpickHeaderRepository cwsSmartpickHeaderRepository;

    @Autowired
    private ConveyorPickCommonService conveyorPickCommonService;

    @Autowired
    private CwsSmartpickDetailRepository cwsSmartpickDetailRepository;

    @Override
    // @Transactional(transactionManager = TransactionMangers.DEFAULT, rollbackFor = Exception.class)
    public boolean inputPickToTote(PickToToteInnerRequest request) {
        Integer locNo = request.getLocNo();
        String toteId = request.getToteId();
        String zone = request.getZone();
        boolean bypassQC = request.bypassQC();
        boolean repackable = request.repackable();
        boolean wrongRepackable = request.getWrongRepackable();
        List<String> cartonList = request.getCartonList();

        // picker scan the tote# ,maybe non-repackable flag is wrong.
        // this.checkCartonNo(request.getToteId());

        if (wrongRepackable) {
            this.insertCwsBinMngLog(request);
        }

        TotePickHeader totePickHeader = this.getTotePickHeader(locNo, toteId, zone);
        totePickHeader = (totePickHeader == null ? new TotePickHeader() : totePickHeader);

        if (CollectionUtils.isEmpty(totePickHeader.getDetails())) {
            throw new BizException("Pick details not found!");
        }

        totePickHeader.setBypassQcCandidateOrder(bypassQC);
        totePickHeader.setOrderAllCanBypassQC(bypassQC);

        List<Map<String, Object>> orderMapList = cwsConveyorPickRepository.getOrderNoAndOrderTypeByCartonIdAndLocNo(toteId, locNo);
        if (CollectionUtils.isNotEmpty(orderMapList)) {
            Map<String, Object> orderMap = orderMapList.get(0);
            Integer orderNo = (Integer) orderMap.get("orderNo");
            Integer orderType = (Integer) orderMap.get("orderType");
            request.setOrderNo(orderNo);
            request.setOrderType(orderType);
        }

        // If bypassQC candidate order and non-repackable part, can bypass QC, now block it.
        if (totePickHeader.isBypassQcCandidateOrder()) {
            LOG.info("totePickHeader.isBypassQcCandidateOrder() return true");
            // throw new BizException("This is repackable part, can't do bypass QC process.");
        } else {
            LOG.info("totePickHeader.isBypassQcCandidateOrder() return false");
        }

        if (repackable) {
            LOG.info("repackable = true");
            if (bypassQC) { // For repackable, only single line and single order qty can go to bypass QC
                LOG.info("bypassQC = true");
                if (CollectionUtils.isNotEmpty(cartonList) && cartonList.size() == 1) {
                    LOG.info("cartonList.size() == 1");
                    // Bypass qc will save carton info, sn, conveyor_auto_ship_queue
                    this.pickRepackable(request, totePickHeader, bypassQC);
                }
            } else {
                LOG.info("bypassQC = false");
                this.pickRepackable(request, totePickHeader, bypassQC);
            }
        } else { // for non-repackable
            LOG.info("repackable = false");
            this.pickNonRepackable(cartonList, request, totePickHeader, bypassQC, wrongRepackable);
        }

        Long unFinishedCount = cwsConveyorPickRepository.getUnFinishedCount(request.getOrderNo(), request.getOrderType(), zone);

        boolean existsUnFinished = unFinishedCount != null && unFinishedCount > 0;

        return !existsUnFinished;
    }

    private CwsCartonProf getRightBoxById(String boxId, Integer locNo) {
        CwsCartonProf box = cwsCartonProfRepository.getByBoxIdAndLocNo(boxId, locNo);

        // if can't find by box_id, try with suffixed by loc_no
        if (box == null) {
            box = cwsCartonProfRepository.getByBoxIdAndLocNo(boxId + "-" + locNo, locNo);
        }

        // if still not found, may use loc_no = -1 box for non BOX_ID_BY_LOC loc#
        List<CwsGenericProf> cwsGenericProfList = cwsGenericProfRepository.findByProfileTypeAndProfileI("BOX_ID_BY_LOC", locNo);
        if (box == null && CollectionUtils.isEmpty(cwsGenericProfList)) {
            box = cwsCartonProfRepository.getByBoxIdAndLocNo(boxId, -1);
        }

        return box;
    }

    private boolean pickRepackable(PickToToteInnerRequest request, TotePickHeader totePickHeader, boolean bypassQC) {
        TotePickDetail currentTotePickDetail = null;

        List<TotePickDetail> totePickDetailList = totePickHeader.getDetails();

        for (TotePickDetail totePickDetail : totePickDetailList) {
            if (totePickDetail.getSkuNo().intValue() == request.getSkuNo().intValue()) {
                currentTotePickDetail = totePickDetail;
                break;
            }
        }

        if (currentTotePickDetail != null) {
            currentTotePickDetail.setBypassQC(bypassQC);
        }

        Integer locNo = request.getLocNo();
        Integer userId = request.getCurrentUserId();
        String toteId = request.getToteId();
        List<String> cartonList = request.getCartonList();

        // if(CollectionUtils.isNotEmpty(cartonList)){}
        if (bypassQC) {
            toteId = cartonList.get(0);
        }
        String zone = request.getZone();
        String binLoc = request.getBinLoc();
        // String curBinlocId = request.getBinLoc();
        String primaryToteId = request.getToteId();
        Integer pickQty = request.getPickQty();
        Integer orderType = totePickHeader.getOrderKey().getOrderType();
        Integer orderNo = totePickHeader.getOrderKey().getOrderNo();
        Integer orderLineNo = currentTotePickDetail != null ? currentTotePickDetail.getOrderLineNo() : null;
        Integer skuNo = currentTotePickDetail != null ? currentTotePickDetail.getSkuNo() : null;
        String boxId = request.getBoxId();

        // ================= changeBox begin =================

        boolean isRepackable = true;

        List<CwsGenericProf> cwsGenericProfList = cwsGenericProfRepository.findByProfileTypeAndProfilec("FORCE_SCAN_BOX_ID", "Y");

        if (CollectionUtils.isNotEmpty(cwsGenericProfList) && currentTotePickDetail != null && bypassQC) {
            String oldBox = "null";
            String newBox = boxId;
            Box newBoxObject = null;

            if (currentTotePickDetail.getBox() != null) {
                oldBox = currentTotePickDetail.getBox().getBoxId();
            }

            if (newBox != null) {
                newBox = newBox.trim().toUpperCase();
                newBox = newBox.replaceAll(".S", ".s");
            }

            if (newBox == null) {
                throw new BizException("Please scan the new Box.");
            }

            // get the right box smartly
            CwsCartonProf newRightBox = this.getRightBoxById(newBox, locNo);
            if (newRightBox == null) {
                throw new BizException(String.format("Input boxId: %s can't be found in system!", newBox));
            }

            newBox = newRightBox.getId().getBoxId();

            double newLength = newRightBox.getLength().doubleValue() * 100;
            double newWidth = newRightBox.getWidth().doubleValue() * 100;
            double newHeight = newRightBox.getHeight().doubleValue() * 100;

            newBoxObject = new Box(newBox, (int) newLength, (int) newWidth, (int) newHeight);

            // save new box into system(cws_smartpick_header), overwrite the old one
            cwsSmartpickHeaderRepository.updateBoxId(orderNo, orderType, newBox);

            currentTotePickDetail.setBox(newBoxObject);

            for (TotePickDetail pickDetail : totePickHeader.getDetails()) {
                pickDetail.setBox(newBoxObject);
            }

            LOG.info("Order#" + orderNo + " change Box id from " + oldBox + " to " + newBox);

            // insert into cws_eventlog
            try {
                CwsEventLog log = new CwsEventLog();
                log.setEventType(CwsConstants.CHANGE_BOX_EVENT);
                log.setEntryDateTime(new Date());
                log.setSourceBin(toteId);
                log.setDestBin("RF Box Force Scan");
                log.setSourceId(oldBox);
                log.setDestId(newBox);
                log.setOrderNo(orderNo);
                log.setOrderType(orderType);
                log.setSkuNo(skuNo);
                log.setDcLocNo(locNo);
                log.setPackerId(userId);
                cwsEventLogRepository.save(log);
            } catch (Exception e) {
                LOG.error("insert data into cws_eventlog failed.");
            }

        }

        // ================= changeBox end =================

        // ================= doPickRepackable logic not done begin =================

        if (currentTotePickDetail != null) {
            if (currentTotePickDetail.isBypassQC() != bypassQC) {
                LOG.error("curTotePickDetail.isBypassQC() != bypassQC, such as " + currentTotePickDetail.isBypassQC() + " != " + bypassQC);
            }
            // currentTotePickDetail.setBypassQC(bypassQC);
        }

        // bypass qc part save carton info, sn, conveyor_auto_ship_queue
        if (currentTotePickDetail != null && currentTotePickDetail.isBypassQC()) {
            if (currentTotePickDetail.isNeedScanSN() && CollectionUtils.isNotEmpty(request.getSnList())) {
                this.saveScannedSns(orderType, orderNo, orderLineNo, toteId, skuNo, request.getSnList(),
                        currentTotePickDetail.getInputSerialIMEI(), currentTotePickDetail.getInputSerialESN(), currentTotePickDetail.getInputSerialICCID(),
                        currentTotePickDetail.getInputSerialMAC(), userId, locNo);
            }

            PartMaster partMaster = null;
            double uWeight = 0;

            JsonEntity<PartMaster> partMasterJsonEntity = partServiceClient.getPartMasterBySkuNo(skuNo);
            if (partMasterJsonEntity != null && partMasterJsonEntity.getData() != null) {
                partMaster = partMasterJsonEntity.getData();
                if (partMaster.getWeight() != null) {
                    uWeight = partMaster.getWeight().doubleValue();
                }
            }

            double tareWeight = 0;
            CwsCartonProf cartonProf = this.getRightBoxById(boxId, locNo);
            if (cartonProf != null) {
                tareWeight = cartonProf.getTareWeight().doubleValue();
            }

            this.insertCartonHeader(orderType, orderNo, toteId, CwsConstants.BYPASS_QC_PACKLANE, uWeight * pickQty + tareWeight, tareWeight, boxId, userId, locNo);

            this.insertCartonDetail(toteId, orderLineNo, skuNo, pickQty, userId);

            // IF CUST# IN NO_PACKING_LIST,drop ship orders (oh.drop_ship = D/Y), order_type =1, No packing list needed
            boolean isNoNeedPrintPL = this.isNoNeedPrintPL(orderType, orderNo);
            String plFlag = isNoNeedPrintPL ? null : "Y";
            this.insertConveyorAutoShipQueue(orderType, orderNo, toteId, CwsConstants.TYPE_REGULAR, CwsConstants.ORDER_READY_GENERATE_LABEL_DATA, locNo, userId, plFlag, "Y", request);
        }

        String needQcZone = "";
        if (currentTotePickDetail != null) {
            needQcZone = currentTotePickDetail.isBypassQC() ? "N" : "Y";
        }
        this.doConveyorPick(locNo, userId, binLoc, zone, primaryToteId, toteId, skuNo, pickQty, "N", needQcZone);

        if (currentTotePickDetail != null) {
            currentTotePickDetail.setPickedQty(currentTotePickDetail.getPickedQty() + pickQty);
            CwsInv inv = currentTotePickDetail.getRecommendBinloc();
            if (inv != null && inv.getId().getBinLoc().equals(binLoc)) {
                inv.setTotQty(inv.getTotQty() - pickQty);
            }
        }

        // check pick complete
        // if (!checkPickToZero(rtn, actions)) {
        //    return false;
        //}

        // check if pick complete

        // order finished pick in current zone, send zone finish pick socket
        OrderKey orderKey = new OrderKey();
        orderKey.setOrderNo(orderNo);
        orderKey.setOrderType(orderType);
        this.sendToZone(locNo, primaryToteId, orderKey, zone, true);

        CwsSmartpickDetailId cwsSmartpickDetailId = new CwsSmartpickDetailId();
        cwsSmartpickDetailId.setOrderNo(orderNo);
        cwsSmartpickDetailId.setOrderType(orderType);
        cwsSmartpickDetailId.setSkuNo(skuNo);
        cwsSmartpickDetailId.setBoxSeq(1);
        Optional<CwsSmartpickDetail> optionalCwsSmartpickDetail = cwsSmartpickDetailRepository.findById(cwsSmartpickDetailId);
        if (optionalCwsSmartpickDetail.isPresent()) {
            CwsSmartpickDetail cwsSmartpickDetail = optionalCwsSmartpickDetail.get();
            cwsSmartpickDetail.setPickDate(new Date());
            if (cwsSmartpickDetail.getPickQty() == null) {
                cwsSmartpickDetail.setPickQty(0);
            }

            if (cwsSmartpickDetail.getPickQty() + pickQty >= cwsSmartpickDetail.getOrderQty()) {
                cwsSmartpickDetail.setPickQty(cwsSmartpickDetail.getOrderQty());
            }
            cwsSmartpickDetail.setPickToCarton(toteId);

            if (cwsSmartpickDetail.getPickQty().equals(cwsSmartpickDetail.getOrderQty())) {
                cwsSmartpickDetail.setPickStatus(CwsSmartpickStatus.COMPLETED);
            }
            if (cwsSmartpickDetail.getPickerId() == null) {
                cwsSmartpickDetail.setPickerId(userId);
            }
            cwsSmartpickDetailRepository.save(cwsSmartpickDetail);
        }

        Long unFinishedCount = cwsConveyorPickRepository.getUnFinishedCount(orderNo, orderType, zone);

        boolean existsUnFinished = unFinishedCount != null && unFinishedCount > 0;

        if (!existsUnFinished) {

            boolean isOrderPickComplete = this.isPickComplete(orderType, orderNo);

            if (isOrderPickComplete) {
                cwsSmartpickHeaderRepository.finishMultiPaperlessPickLine(orderNo, orderType, bypassQC ? "BYPASSQC" : "Conveyor");
            }

            // An order may be picked at several zones,need to check the whole order is picked completely.
            // simon requested.
            if (isOrderPickComplete && bypassQC) {
            //  if (isOrderPickComplete && (totePickHeader.isBypassQcCandidateOrder())) {
                // send order carton to tape_zone(110)/pl_print_zone(120)/label_zone(130).
                // this.insertConveyorSocketByShipQueue(locNo, orderType, orderNo);

                // after order pick completely, if order all carton go to bypass QC, assgin order qc_date
                if (totePickHeader.isOrderAllCanBypassQC()
                        && CollectionUtils.isEmpty(cwsConveyorSocketRepository.existsManuallyTote(orderNo, orderType))) {
                    List<CartonHeader> cartonDatas = cartonHeaderRepository.findByOrderNoAndOrderType(orderNo, orderType);
                    int cartonCount;
                    int boxSeq = 0;
                    if (CollectionUtils.isNotEmpty(cartonDatas)) {
                        cartonCount = cartonDatas.size();
                        for (int i = 0; i < cartonCount; i++) {
                            CartonHeader carton = cartonDatas.get(i);
                            boxSeq++;
                            cartonHeaderRepository.updateBoxSeq(boxSeq, cartonCount, carton.getCartonNo());
                        }
                    }
                    this.updateQCDate(orderType, orderNo);
                }
            }

            // if tote is empty and don't have any plan, delete it from socket
            Long toteFreeCount = cwsConveyorPickRepository.isToteFree(locNo, primaryToteId);

            boolean isToteFree = (toteFreeCount == null || toteFreeCount == 0);

            if (isToteFree) {
                this.removeToteFromConveyor(locNo, primaryToteId, orderType, orderNo);

                // Move tote to history
                cwsConveyorPickHistoryRepository.insertData(locNo, primaryToteId);
                cwsConveyorPickRepository.deleteByLocNoAndCartonId(primaryToteId, locNo);

                // String msg = "Tote has no task and is empty now.\nPls don't put it back to conveyor. It can be used as an new empty tote.";
                // rtn.addDialogInfoMsgNoAlter("Tote is Free", msg);
            }
        }
        return true;
    }

    @Transactional(transactionManager = TransactionMangers.DEFAULT, rollbackFor = Exception.class)
    private void saveScannedSns(Integer orderType, Integer orderNo, Integer orderLineNo, String cartonNo, Integer skuNo, List<String> serialNos,
                                HashMap<String, String> serialIMEI, HashMap<String, String> serialESN, HashMap<String, String> serialICCID,
                                HashMap<String, String> serialMAC, Integer userId, Integer locNo) {
        if (CollectionUtils.isEmpty(serialNos)) {
            return;
        }

        for (String serialNo : serialNos) {
            String imeiNo = ObjectUtil.isEmpty(serialIMEI) ? "" : serialIMEI.get(serialNo);
            String esnNo = ObjectUtil.isEmpty(serialESN) ? "" : serialESN.get(serialNo);
            String iccidNo = ObjectUtil.isEmpty(serialICCID) ? "" : serialICCID.get(serialNo);
            String macAddress = ObjectUtil.isEmpty(serialMAC) ? "" : serialMAC.get(serialNo);

            SerialNbrId id = new SerialNbrId(orderNo, orderType, orderLineNo, serialNo, cartonNo);
            SerialNbr serialNbr = new SerialNbr();

            serialNbr.setId(id);
            serialNbr.setSkuNo(skuNo);
            serialNbr.setEntryDatetime(new Date());

            serialNbr.setEntryId(userId);
            serialNbr.setLocNo(locNo);

            serialNbr.setImeiNo(imeiNo);
            serialNbr.setEsnNo(esnNo);
            serialNbr.setIccidNo(iccidNo);
            serialNbr.setMacAddress(macAddress);

            serialNbrRepository.save(serialNbr);
        }
    }

    private TotePickHeader getTotePickHeader(int locNo, String toteId, String zone) {

        TotePickHeader header = new TotePickHeader();

        List<Map<String, Object>> mapList = cwsConveyorPickRepository.getMaxArrivalDate(locNo, toteId, zone);

        if (CollectionUtils.isEmpty(mapList)) {
            return null;
        }

        Map<String, Object> orderMap = mapList.get(0);

        OrderKey orderKey = new OrderKey();
        orderKey.setOrderNo(CwsNumberUtil.toInt(orderMap.get("orderNo")));
        orderKey.setOrderType(CwsNumberUtil.toInt(orderMap.get("orderType")));

        header.setOrderKey(orderKey);
        header.setArrivalDate((Date) orderMap.get("arrivalDate"));
        header.setToteId(toteId);
        header.setZone(zone);

        Integer orderType = orderKey.getOrderType();
        Integer orderNo = orderKey.getOrderNo();

        CwsSmartpickHeaderId cwsSmartpickHeaderId = new CwsSmartpickHeaderId();
        cwsSmartpickHeaderId.setOrderNo(orderNo);
        cwsSmartpickHeaderId.setOrderType(orderType);

        boolean isPaperlessOrder;

        Optional<CwsSmartpickHeader> cwsSmartPickHeaderOptional = cwsSmartpickHeaderRepository.findById(cwsSmartpickHeaderId);
        if (cwsSmartPickHeaderOptional.isPresent()) {
            CwsSmartpickHeader cwsSmartpickHeader = cwsSmartPickHeaderOptional.get();
            isPaperlessOrder = cwsSmartpickHeader.getStatus() == null || !"E".equals(cwsSmartpickHeader.getStatus());
            header.setPaperlessOrder(isPaperlessOrder);
        }

        OrderParamBase orderInfoParam = new OrderParamBase();
        orderInfoParam.setOrderType(orderType);
        orderInfoParam.setOrderNo(orderNo);
        List<OrderParamBase> orderInfoParams = new ArrayList<>();
        orderInfoParams.add(orderInfoParam);

        OrderHeader oh = null;
        JsonEntity<List<OrderHeader>> currentOrderHeadersJsonEntity = orderServiceClient.getCurrentOrderHeaders(orderInfoParams);
        if (currentOrderHeadersJsonEntity != null && CollectionUtils.isNotEmpty(currentOrderHeadersJsonEntity.getData())) {
            List<OrderHeader> orderHeaderList = currentOrderHeadersJsonEntity.getData();
            oh = orderHeaderList.get(0);
            if (oh != null) {
                initTotePickHeader(oh, header);
            }
        }

        List<Map<String, Object>> detailMapList = cwsConveyorPickRepository.getDetail(locNo, toteId, zone);
        List<TotePickDetail> details = new ArrayList<>();

        boolean isBypassQCWh = false;
        boolean orderNeedQcSticker = false;
        boolean custExcludeBypassQC = false;
        boolean orderConveyorBypassExcludeCarrier = false;
        boolean orderValidBypassQcSpecialHandle = true;
        boolean existIPVPPart = false;
        boolean noShipmethodMap = false;
        boolean isBeyondLineLimit = false;
        boolean isBeyondQtyLimit = false;
        boolean hasTonerPart = false;

        Order order = orderService.getCurrentOrderInfo(orderType, orderNo);

        Set<String> profileTypes = new HashSet<>();
        profileTypes.add("BYPASS_QC");
        List<WhProfile> whProfiles = whProfileRepository.getByLocNoAndProfileTypes(locNo, profileTypes);
        if (CollectionUtils.isNotEmpty(whProfiles)) {
            isBypassQCWh = true;
        }

        if (isBypassQCWh) {

            List<String> intangibles = cwsGenericProfRepository.getProfileCList("IntangiblePart");
            if (CollectionUtils.isNotEmpty(intangibles)) {
                String intangible = intangibles.get(0);
                if ("Y".equals(intangible)) {
                    existIPVPPart = this.existIPVPPart(order);
                }
            }

            if (oh != null && oh.getToAcctNo() != null) {
                List<CwsGenericProf> stickerProfiles = cwsGenericProfRepository.findByProfileTypeAndProfileI("QC_Sticker", oh.getToAcctNo());
                if (CollectionUtils.isNotEmpty(stickerProfiles)) {
                    orderNeedQcSticker = true;
                }

                List<CwsGenericProf> excludeCustProfiles = cwsGenericProfRepository.findByProfileTypeAndProfileI("BypassQC_Exclude_Cust", oh.getToAcctNo());
                if (CollectionUtils.isNotEmpty(excludeCustProfiles)) {
                    custExcludeBypassQC = true;
                }

                List<String> profileTypeList = new ArrayList<>();
                profileTypeList.add("HP_Toner");
                profileTypeList.add("Lexmark_Toner");
                List<CwsGenericProf> cwsGenericProfList = cwsGenericProfRepository.findByProfileTypeListAndProfileI(profileTypeList, oh.getToAcctNo());
                if (CollectionUtils.isNotEmpty(cwsGenericProfList)) {
                    List<Integer> integerList = cwsCartonProfRepository.existsTonerPart(orderNo, orderType);
                    if (CollectionUtils.isNotEmpty(integerList)) {
                        hasTonerPart = true;
                    }
                }
            }

            int lineLimit = Integer.MAX_VALUE;
            int qtyLimit = Integer.MAX_VALUE;

            List<CwsGenericProf> BypassQcLineLimit = cwsGenericProfRepository.findByProfileType("BYPASS_QC_MAX_LINE");
            if (CollectionUtils.isNotEmpty(BypassQcLineLimit)) {
                lineLimit = BypassQcLineLimit.get(0).getProfileI();
            }

            List<CwsGenericProf> BypassQcQtyLimit = cwsGenericProfRepository.findByProfileType("BYPASS_QC_MAX_QTY");
            if (CollectionUtils.isNotEmpty(BypassQcQtyLimit)) {
                qtyLimit = BypassQcQtyLimit.get(0).getProfileI();
            }

            List<CwsPickProgress> progs = cwsPickProgressService.getConsolidatedProgress(orderType, orderNo);
            if (CollectionUtils.isNotEmpty(progs) && progs.size() > lineLimit) {
                isBeyondLineLimit = true;
            }

            for (int i = 0; CollectionUtils.isNotEmpty(progs) && i < progs.size(); i++) {
                if (progs.get(i).getOrderQty() > qtyLimit) {
                    isBeyondQtyLimit = true;
                    break;
                }
            }

            if (oh != null && oh.getShipMethod() != null) {
                List<String> carrierList = shipMethodMapRepository.getCarriersByShipMethod(oh.getShipMethod());
                if (CollectionUtils.isNotEmpty(carrierList)) {
                    List<CwsGenericProf> excludeCarrierProfiles = cwsGenericProfRepository.findByProfileTypeAndProfilec("ConveyorBypassExcludeCarrier", carrierList.get(0));
                    if (CollectionUtils.isNotEmpty(excludeCarrierProfiles)) {
                        CwsGenericProf prof = excludeCarrierProfiles.get(0);
                        List<String> shipMethodList = (prof.getProfileC2() != null) ? Arrays.asList(prof.getProfileC2().replace(" ", "").split(",")) : new ArrayList<>();
                        if (shipMethodList.size() == 0 || prof.getProfileC2().equals(oh.getShipMethod().trim()) || shipMethodList.contains(oh.getShipMethod().trim())) {
                            orderConveyorBypassExcludeCarrier = true;
                        }
                    }
                } else {
                    noShipmethodMap = true;
                }
            }

            List<String> onlyPaperPickSpecialHandles = cwsGenericProfRepository.getProfileCListNotNull("BypassQC_Exclude_Spec");
            if (onlyPaperPickSpecialHandles != null && onlyPaperPickSpecialHandles.size() > 0) {
                List<OrderProfile> orderProfiles = getOrderProfiles(order.getOrderHeader());
                for (int i = 0; i < orderProfiles.size(); i++) {
                    if (onlyPaperPickSpecialHandles.contains(orderProfiles.get(i).getProfileC())) {
                        orderValidBypassQcSpecialHandle = false;
                        break;
                    }
                }
                if (order.getOrderHeader().getOrderSoldTo() != null) {
                    if (onlyPaperPickSpecialHandles.contains(CwsStringUtil.trimStr(order.getOrderHeader().getOrderSoldTo().getSpecialHandle()))) {
                        orderValidBypassQcSpecialHandle = false;
                    }
                }

            }

//            List<String> onlyPaperPickSpecialHandles = cwsGenericProfRepository.getProfileCListNotNull("BypassQC_Exclude_Spec");
//            if (CollectionUtils.isNotEmpty(onlyPaperPickSpecialHandles)) {
//
//                OrderProfile orderProfile = new OrderProfile();
//
//                OrderProfileId orderProfileId = new OrderProfileId();
//                orderProfileId.setOrderNo(orderNo);
//                orderProfileId.setOrderType(orderType);
//                orderProfileId.setProfileType("SPEC_HANDL");
//                orderProfileId.setProfileCat("HDLG");
//
//                orderProfile.setOrderProfileId(orderProfileId);
//
//                JsonEntity<List<OrderProfile>> orderProfileListJsonEntity = orderServiceClient.getOrderProfilesByProfileParam(orderProfile);
//                if (orderProfileListJsonEntity != null && CollectionUtils.isNotEmpty(orderProfileListJsonEntity.getData())) {
//                    List<OrderProfile> orderProfiles = orderProfileListJsonEntity.getData();
//                    for (OrderProfile profile : orderProfiles) {
//                        if (onlyPaperPickSpecialHandles.contains(profile.getProfileC())) {
//                            orderValidBypassQcSpecialHandle = false;
//                            break;
//                        }
//                    }
//                }
//
//                if (onlyPaperPickSpecialHandles.contains(header.getSpecialHandle())) {
//                    orderValidBypassQcSpecialHandle = false;
//                }
//            }
        }

        /**
         * 1) Filter all deleted/zero qty line .(if deleted line is kit, then also deledted its components)
         * 2) Filter out bundled-kit's component.
         * 3) Filter out virutal kit part (non-bundled)
         */
        List<OrderDetail> orderDetailList = orderService.getOrderDetailList(orderType, orderNo);
        List<OrderDetail> orderDetails = conveyorPickInjectService.getFilterOrderDetails(orderDetailList);

        // 5/27/19 simonc, Battery part do not allow conveyor bypass QC
        // 12/10/19,simonc, [CISCWS-1572], Do not allow Hazmat order go to bypass QC
        boolean hasBatteryPart = false;
        boolean hasHazPart = false;
        for (OrderDetail orderDetail : orderDetails) {
            List<CwsPart> cwsPartList = cwsPartRepository.findBySkuNo(orderDetail.getSkuNo());
            if (CollectionUtils.isNotEmpty(cwsPartList)) {
                CwsPart cwsPart = cwsPartList.get(0);
                if ("Y".equalsIgnoreCase(CwsStringUtil.trimToEmptyString(cwsPart.getBatteryFlag()))) {
                    hasBatteryPart = true;
                    break;
                }
                Integer skuNo = cwsPart.getSkuNo();
                if (CollectionUtils.isNotEmpty(skuProfileRepository.isHazmatPart(skuNo))) {
                    hasHazPart = true;
                    break;
                }
            }
        }

        if (isBypassQCWh && !orderNeedQcSticker && !custExcludeBypassQC && !orderConveyorBypassExcludeCarrier && orderValidBypassQcSpecialHandle && !existIPVPPart
                && !noShipmethodMap && !hasBatteryPart && !isBeyondLineLimit && !isBeyondQtyLimit && !hasHazPart && !hasTonerPart) {
            header.setBypassQcCandidateOrder(true);

            // see if order all part can be bypass qc
            boolean allCanBypassQC = true;
            int orderLineCount = orderDetails.size();

            for (OrderDetail orderDetail : orderDetails) {
                List<CwsPart> cwsPartList = cwsPartRepository.findBySkuNo(orderDetail.getSkuNo());
                if (CollectionUtils.isNotEmpty(cwsPartList)) {
                    CwsPart cwsPart = cwsPartList.get(0);
                    if (cwsPart.getRepackable() != null && "Y".equals(cwsPart.getRepackable())) {
                        if (orderLineCount > 1 || (orderLineCount == 1 && orderDetail.getOrderQty() > 1)) {
                            allCanBypassQC = false;
                            break;
                        }
                    }
                }
            }
            header.setOrderAllCanBypassQC(allCanBypassQC);
        }

        for (Map<String, Object> detailMap : detailMapList) {
            TotePickDetail detail = populateTotePickDetail(detailMap, CwsNumberUtil.toInt(oh.getToAcctNo()));
            for (OrderDetail orderDetail : orderDetails) {
                if (orderDetail.getSkuNo().intValue() == detail.getSkuNo().intValue()) {
                    detail.setOrderLineNo(orderDetail.getId().getOrderLineNo());
                    break;
                }
            }

            // validate the detail can be bypass qc or not
            // is BYPASS_QC wh
            // Order doesn't have QC_Sticker
            // Special_handle not in(RL,SI)
            if (header.isBypassQcCandidateOrder()) {
                if (!detail.isRepackable()) {
                    detail.setBypassQC(true);
                }
                if (detail.isRepackable()) {
                    // whole order single sku single qty repackable can go to bypass qc
                    if (orderDetails != null && orderDetails.size() == 1) {
                        for (OrderDetail d : orderDetails) {
                            if (d.getOrderQty() == 1) {
                                detail.setBypassQC(true);
                            }
                        }
                    }
                }
            }

            // if bypass qc part and need scan sn, get sn profile
            if (detail.isBypassQC() && detail.isNeedScanSN()) {
                List<SerNoProf> serialNoProfiles = this.getSNProfileListForShip(detail.getVendNo(), detail.getMfgPartNo(), detail.getDcPartId());
                if (CollectionUtils.isNotEmpty(serialNoProfiles)) {
                    detail.setSerialNoProfiles(serialNoProfiles);
                    detail.setMultiPackQty(serialNoProfiles.get(0).getMultiPackQty());
                }
            }

            details.add(detail);
        }

        header.setDetails(details);


        return header;
    }

    private boolean existIPVPPart(Order order) {
        boolean existIPVPPart = false;
        for (OrderDetail orderDetail : order.getOrderDetails()) {
            PartMaster part = orderService.getPartBySku(orderDetail.getSkuNo());
            if (part != null && ("IP".equals(part.getUsageType()) || "VP".equals(part.getUsageType()))) {
                existIPVPPart = true;
                break;
            }
        }
        return existIPVPPart;
    }

    private List<OrderProfile> getOrderProfiles(OrderHeader oh) {
        Integer orderType = oh.getId().getOrderType();
        Integer orderNo = oh.getId().getOrderNo();

//		select * from order_profile where profile_type = 'SPEC_HANDL'
//		and order_type = :orderType and order_no = :orderNo
//		and profile_c = 'HDLG' and active = 'Y'
        OrderInfoParam queryForm = new OrderInfoParam();
        queryForm.setOrderType(orderType);
        queryForm.setOrderNo(orderNo);
        List<String> profileTypes = new ArrayList<String>();
        profileTypes.add("SPEC_HANDL");
        queryForm.setProfileTypes(profileTypes);
        List<OrderInfoParam> orderInfoParams = new ArrayList<OrderInfoParam>();
        orderInfoParams.add(queryForm);
        List<OrderProfile> profileList = orderServiceClient.getCurrentOrderProfilesInfo(orderInfoParams).getData();
        if (!ObjectUtil.isEmpty(profileList)) {
            profileList.stream().filter(
                    op -> ("" + op.getProfileC()).equalsIgnoreCase("HDLG") && ("" + op.getActive()).equalsIgnoreCase("Y"))
                    .collect(Collectors.toList());
        }
        return profileList;
    }

    public List<SerNoProf> getSNProfileListForShip(Integer vendNo, String mfgPartNo, String dcPartId) {
        if (dcPartId == null || dcPartId.trim().length() == 0) {
            dcPartId = mfgPartNo;
        }

        return serNoProfRepository.getActiveProfList(vendNo, new String[]{mfgPartNo, dcPartId}, new String[]{"S", "B"});
    }

    private String encodeStringForUI(String str) {
        if (str != null) {
            str = str.replace("\"", "'");
        }

        return str;
    }

    private boolean getBoolFromChar(Object obj) {
        if (obj == null || obj.toString().trim().length() == 0) {
            return false;
        }

        String str = obj.toString().trim();
        return "Y".equalsIgnoreCase(str)
                || "YES".equalsIgnoreCase(str)
                || "T".equalsIgnoreCase(str)
                || "TRUE".equalsIgnoreCase(str);
    }

    // josephj, 10/24/2018, remove customer EXEMPT_SN checking logic, requested by Mike Nolan
    public boolean isSerialNoExempt(Integer custNo) {
        return false;
        // CustProfile prof = getCustProfileDAO().findById(new CustProfileId(custNo, "EXEMPT_SN", "OTHE"));
        // return prof != null && ("" + prof.getActive()).trim().equals("Y") && ("" + prof.getProfileC()).trim().equalsIgnoreCase("Y");
    }

    private TotePickDetail populateTotePickDetail(Map<String, Object> map, Integer custNo) {
        TotePickDetail detail = new TotePickDetail();
        detail.setSkuNo((Integer) map.get("skuNo"));
        detail.setInvType((Integer) map.get("invType"));
        detail.setZone((String) map.get("zone"));
        detail.setToteId((String) map.get("cartonId"));
        detail.settestPartNo((String) map.get("partNo"));
        detail.setMfgPartNo((String) map.get("mfgPartno"));
        detail.setUpcCode((String) map.get("upcCode"));
        detail.setUpcCode2((String) map.get("upcCode2"));
        detail.setUpcCode3((String) map.get("upcCode3"));
        detail.setShortDesc(this.encodeStringForUI((String) map.get("shortDesc")));
        detail.setVendNo((Integer) map.get("vendNo"));
        detail.setImageName((String) map.get("imageName"));
        detail.setPlannedPickQty((Integer) map.get("pickQtyPlanned"));
        detail.setPickedQty((Integer) map.get("pickQtyActual"));
        detail.setRepackable(this.getBoolFromChar(map.get("repackable")));
        detail.setDcPartId((String) map.get("dcPartId"));

        // boolean needScanSN = !ObjectUtil.isEmpty(map.get("serNoFlag")) && !"N".equals(map.get("serNoFlag"));

        boolean needScanSN = (ObjectUtil.isEmpty(map.get("serNoFlag")) || "N".equals(map.get("serNoFlag"))) ? false : true;

        List<Integer> profileIList = cwsGenericProfRepository.getProfileIList("SN_Enforce_Vend");
        boolean isEnforceScanSNVendor = CollectionUtils.isNotEmpty(profileIList) && profileIList.contains(detail.getVendNo());

        boolean isSerialNoExempt = this.isSerialNoExempt(custNo);

        if (needScanSN && (isEnforceScanSNVendor || !isSerialNoExempt)) {
            detail.setNeedScanSN(true);
        }

        return detail;
    }

    private void initTotePickHeader(OrderHeader pph, TotePickHeader tph) {
        tph.setFromInvType(pph.getFromInvType());
        tph.setDeleteDate(pph.getDeleteDate());
        tph.setFromLocNo(pph.getFromLocNo());
        tph.setPickDate(pph.getPickDate());
        tph.setQcDate(pph.getQcDate());
        tph.setShipDate(pph.getShipDate());
        tph.setShipMethod(pph.getShipMethod());
        tph.setSpecialHandle(this.getSpecialHandle(pph));
        tph.setToAccountNo(CwsNumberUtil.toInt(pph.getToAcctNo()));
    }

    /**
     * To prevent nullpoint issue, return "" when the order_soldto do not exist or special_hanlde is null.
     */
    private String getSpecialHandle(OrderHeader order) {
        if (order == null) {
            return "";
        }

        OrderSoldTo osd = order.getOrderSoldTo();
        // return "" if order_soldto not exists.
        if (osd == null) {
            return "";
        }

        return osd.getSpecialHandle() == null ? "" : osd.getSpecialHandle().trim();
    }


    /**
     * When return false, need return to ui directly.
     */
    private boolean pickNonRepackable(List<String> cartons, PickToToteInnerRequest request, TotePickHeader totePickHeader, boolean bypassQC, boolean wrongRepackable) {
        boolean currentZonePickDone = false;

//        boolean existMannullyCarton = false;

//        Integer locNo = request.getLocNo();
//        String toteId = request.getToteId();
//        String zone = request.getZone();
//        String binLoc = request.getBinLoc();
//        Integer invType = request.getInvType();
//        Integer pickQty = request.getPickQty();
//        Integer skuNo = request.getSkuNo();
//        Integer orderNo = request.getOrderNo();
//        Integer orderType = request.getOrderType();
//        List<String> cartonList = request.getCartonList();

        TotePickDetail curTotePickDetail = null;

        List<TotePickDetail> totePickDetailList = totePickHeader.getDetails();

        if (CollectionUtils.isNotEmpty(totePickDetailList)) {
            for (TotePickDetail totePickDetail : totePickDetailList) {
                if (totePickDetail.getSkuNo().intValue() == request.getSkuNo().intValue()) {
                    curTotePickDetail = totePickDetail;
                    break;
                }
            }
        }

        if (curTotePickDetail != null) {
            curTotePickDetail.setBypassQC(bypassQC);
        }

        //        if (curTotePickDetail != null) {
        //            if (curTotePickDetail.isBypassQC() != bypassQC) {
        //                LOG.error("curTotePickDetail.isBypassQC() != bypassQC, such as " + curTotePickDetail.isBypassQC() + " != " + bypassQC);
        //            }
        //            // curTotePickDetail.setBypassQC(bypassQC);
        //        }

        // bypassQC = curTotePickDetail != null ? curTotePickDetail.isBypassQC() : bypassQC;

        // String forceNonRepackable = (curTotePickDetail != null && curTotePickDetail.isRepackable()) ? "Y" : "N";
        String forceNonRepackable = wrongRepackable ? "Y" : "N";

        Integer locNo = request.getLocNo();
        Integer userid = request.getCurrentUserId();
        String toteId = request.getToteId();
        String zone = request.getZone();
        String binLoc = request.getBinLoc();
        String curBinlocId = request.getBinLoc();
        String primaryToteId = request.getToteId();
        Integer pickQty = request.getPickQty();
        Integer orderType = totePickHeader.getOrderKey().getOrderType();
        Integer orderNo = totePickHeader.getOrderKey().getOrderNo();
        Integer orderLineNo = curTotePickDetail != null ? curTotePickDetail.getOrderLineNo() : null;
        Integer skuNo = curTotePickDetail != null ? curTotePickDetail.getSkuNo() : null;
        boolean existMannullyCarton = false;

        if (CollectionUtils.isNotEmpty(cartons)) {
            for (String nonRepackableCartonNo : cartons) {
                // bypass qc part save carton info, sn, conveyor_auto_ship_queue
                if (curTotePickDetail != null && curTotePickDetail.isBypassQC()) {
                    if (curTotePickDetail.isNeedScanSN() && CollectionUtils.isNotEmpty(request.getSnList())) {
                        this.saveScannedSns(totePickHeader.getOrderKey().getOrderType(), totePickHeader.getOrderKey().getOrderNo(),
                                orderLineNo, nonRepackableCartonNo, skuNo, request.getSnList(),
                                curTotePickDetail.getInputSerialIMEI(), curTotePickDetail.getInputSerialESN(), curTotePickDetail.getInputSerialICCID(),
                                curTotePickDetail.getInputSerialMAC(), userid, locNo);
                    }

                    double uWeight = 0;
                    JsonEntity<List<PartMaster>> partMasterListJsonEntity = partServiceClient.getPartMasterBySkuNos(skuNo + "");
                    if (partMasterListJsonEntity != null && CollectionUtils.isNotEmpty(partMasterListJsonEntity.getData())) {
                        List<PartMaster> partMasterList = partMasterListJsonEntity.getData();
                        PartMaster partMaster = partMasterList.get(0);
                        if (partMaster != null && partMaster.getWeight() != null) {
                            uWeight = partMaster.getWeight().doubleValue();
                        }
                    }

                    this.insertCartonHeader(orderType, orderNo, nonRepackableCartonNo, CwsConstants.BYPASS_QC_PACKLANE, uWeight * pickQty, 0, null, userid, locNo);

                    this.insertCartonDetail(nonRepackableCartonNo, orderLineNo, skuNo, pickQty, userid);
                }

                String needQcZone = "";

                if (curTotePickDetail != null) {
                    needQcZone = curTotePickDetail.isBypassQC() ? "N" : "Y";
                    this.doConveyorPick(locNo, userid, binLoc, zone, primaryToteId, nonRepackableCartonNo, curTotePickDetail.getSkuNo(), 1, forceNonRepackable, needQcZone);
                }

                if (curTotePickDetail != null && curTotePickDetail.isBypassQC()) {
                    String plFlag;
                    if (totePickHeader.isOrderAllCanBypassQC() && this.isPickComplete(orderType, orderNo)) {
                        if (CollectionUtils.isNotEmpty(cwsConveyorSocketRepository.existMannullyCarton(locNo, orderNo, orderType))) {
                            existMannullyCarton = true;
                        } else {
                            plFlag = "Y";
                        }
                    }

                    // IF CUST# IN NO_PACKING_LIST,drop ship orders (oh.drop_ship = D/Y), order_type =1, No packing list needed
                    boolean isNoNeedPrintPL = this.isNoNeedPrintPL(orderType, orderNo);
                    plFlag = isNoNeedPrintPL ? null : "Y";

                    this.insertConveyorAutoShipQueue(orderType, orderNo, nonRepackableCartonNo, CwsConstants.TYPE_REGULAR, CwsConstants.ORDER_READY_GENERATE_LABEL_DATA, locNo, request.getCurrentUserId(), plFlag, null, request);
                }

            }
        }

        if (curTotePickDetail != null) {
            curTotePickDetail.setPickedQty(curTotePickDetail.getPickedQty() + pickQty);
            CwsInv inv = curTotePickDetail.getRecommendBinloc();
            if (inv != null && inv.getId().getBinLoc().equals(binLoc)) {
                inv.setTotQty(inv.getTotQty() - pickQty);
            }
        }

        // check pick complete
        // if (!checkPickToZero(rtn, actions)) {
        //    return false;
        //}

        // order finished pick in current zone, send zone finish pick socket
        OrderKey orderKey = new OrderKey();
        orderKey.setOrderNo(orderNo);
        orderKey.setOrderType(orderType);
        this.sendToZone(locNo, primaryToteId, orderKey, zone, true);

        CwsSmartpickDetailId cwsSmartpickDetailId = new CwsSmartpickDetailId();
        cwsSmartpickDetailId.setOrderNo(orderNo);
        cwsSmartpickDetailId.setOrderType(orderType);
        cwsSmartpickDetailId.setSkuNo(skuNo);
        cwsSmartpickDetailId.setBoxSeq(1);
        Optional<CwsSmartpickDetail> optionalCwsSmartpickDetail = cwsSmartpickDetailRepository.findById(cwsSmartpickDetailId);
        if (optionalCwsSmartpickDetail.isPresent()) {
            CwsSmartpickDetail cwsSmartpickDetail = optionalCwsSmartpickDetail.get();
            cwsSmartpickDetail.setPickDate(new Date());
            if (cwsSmartpickDetail.getPickQty() == null) {
                cwsSmartpickDetail.setPickQty(0);
            }
            if (cwsSmartpickDetail.getPickQty() + pickQty >= cwsSmartpickDetail.getOrderQty()) {
                cwsSmartpickDetail.setPickQty(cwsSmartpickDetail.getOrderQty());
            }

            cwsSmartpickDetail.setPickToCarton(cartons.get(0));

            if (cwsSmartpickDetail.getPickQty().equals(cwsSmartpickDetail.getOrderQty())) {
                cwsSmartpickDetail.setPickStatus(CwsSmartpickStatus.COMPLETED);
            }
            if (cwsSmartpickDetail.getPickerId() == null) {
                cwsSmartpickDetail.setPickerId(userid);
            }
            cwsSmartpickDetailRepository.save(cwsSmartpickDetail);
        }

        // check if pick complete

        Long unFinishedCount = cwsConveyorPickRepository.getUnFinishedCount(orderNo, orderType, zone);

        boolean existsUnFinished = unFinishedCount != null && unFinishedCount > 0;

        if (!existsUnFinished) {
            // order finished pick in current zone, send zone finish pick socket
            // An order may be picked at several zones,need to check the whole order is picked completely.

            boolean isOrderPickComplete = this.isPickComplete(orderType, orderNo);

            if (isOrderPickComplete) {
                cwsSmartpickHeaderRepository.finishMultiPaperlessPickLine(orderNo, orderType, bypassQC ? "BYPASSQC" : "Conveyor");
            }

            if (isOrderPickComplete && (totePickHeader.isBypassQcCandidateOrder())) {
                // send order carton to tape_zone(110)/pl_print_zone(120)/label_zone(130).
                // this.insertConveyorSocketByShipQueue(locNo, orderType, orderNo);

                // after order pick completely, if order all carton go to bypass QC, assgin order qc_date
                if (totePickHeader.isOrderAllCanBypassQC()
                        && !existMannullyCarton
                        && CollectionUtils.isEmpty(cwsConveyorSocketRepository.existMannullyCarton(locNo, orderNo, orderType))
                        && CollectionUtils.isEmpty(cwsConveyorSocketRepository.existsManuallyTote(orderNo, orderType))) {
                    List<CartonHeader> cartonDatas = cartonHeaderRepository.findByOrderNoAndOrderType(orderNo, orderType);
                    int cartonCount;
                    int boxSeq = 0;
                    if (CollectionUtils.isNotEmpty(cartonDatas)) {
                        cartonCount = cartonDatas.size();
                        for (int i = 0; i < cartonCount; i++) {
                            CartonHeader carton = cartonDatas.get(i);
                            boxSeq++;
                            cartonHeaderRepository.updateBoxSeq(boxSeq, cartonCount, carton.getCartonNo());
                        }
                    }
                    this.updateQCDate(orderType, orderNo);
                }
            }

            // if tote is empty and don't have any plan, delete it from socket
            Long toteFreeCount = cwsConveyorPickRepository.isToteFree(locNo, primaryToteId);
            boolean isToteFree = (toteFreeCount == null || toteFreeCount == 0);

            if (isToteFree) {
                this.removeToteFromConveyor(locNo, primaryToteId, orderType, orderNo);

                // Move tote to history
                cwsConveyorPickHistoryRepository.insertData(locNo, primaryToteId);
                cwsConveyorPickRepository.deleteByLocNoAndCartonId(primaryToteId, locNo);

                // String msg = "Tote has no task and is empty now.\nPls don't put it back to conveyor. It can be used as an new empty tote.";
                // rtn.addDialogInfoMsgNoAlter("Tote is Free", msg);
            }

        }

        return currentZonePickDone;
    }

    private void initPickBin(TotePickHeader header, PickToToteInnerRequest request) throws Exception {

        List<Map<String, Object>> list = conveyorZone9Sort.getToteBinloc(request.getLocNo(), request.getInvType(), request.getToteId(), request.getZone());

        List<List<Object>> bins = new ArrayList<>();
        List<TotePickDetail> details = header.getDetails();
        // clear existing recommend bin
        for (int j = 0; j < details.size(); j++) {
            TotePickDetail detail = details.get(j);
            detail.setRecommendBinloc(null);
        }

        int lastSku = 0;
        for (int i = 0; i < bins.size(); i++) {
            List<Object> bin = bins.get(i);
            int skuNo = ((Integer) bin.get(0)).intValue();
            String binloc = (String) bin.get(1);
            int binQty = ((Integer) bin.get(2)).intValue();
            if (lastSku == skuNo) {
                continue;
            }

            lastSku = skuNo;
            for (int j = 0; j < details.size(); j++) {
                TotePickDetail detail = details.get(j);
                if (detail.getSkuNo().intValue() == skuNo) {
                    CwsInv inv = new CwsInv();
                    inv.setId(new CwsInvId(request.getLocNo(), binloc, skuNo));
                    inv.setTotQty(binQty);
                    detail.setRecommendBinloc(inv);
                    break;
                }
            }
        }
    }

    @Transactional(transactionManager = TransactionMangers.DEFAULT, rollbackFor = Exception.class)
    private void insertCartonDetail(String cartonNo, Integer orderLine, Integer skuNo, Integer qty, Integer entryId) {
        CartonDetailId id = new CartonDetailId();
        id.setCartonNo(cartonNo);
        id.setCartonLineNo(orderLine);

        CartonDetail cartonDetail;

        List<CartonDetail> cartonDetailList = cartonDetailRepository.getByCartonNoAndCartonLineNo(cartonNo, orderLine);

        if (CollectionUtils.isEmpty(cartonDetailList)) {
            cartonDetail = new CartonDetail();
            cartonDetail.setId(id);
            cartonDetail.setSkuNo(skuNo);
            cartonDetail.setCrUid(entryId);
            cartonDetail.setItemQty(qty);
            cartonDetail.setCrDttm(new Date());
        } else {
            cartonDetail = cartonDetailList.get(0);
            int oldQty = cartonDetail.getItemQty();
            cartonDetail.setItemQty(oldQty + qty);
        }

        cartonDetailRepository.save(cartonDetail);
    }

    @Transactional(transactionManager = TransactionMangers.DEFAULT, rollbackFor = Exception.class)
    private void updateQCDate(Integer orderType, Integer orderNo) {
        List<CwsGenericProf> cwsGenericProfs = cwsGenericProfRepository.findByProfileTypeAndProfilec("NotSetQcDateForBypassQC", "Y");
        if (CollectionUtils.isNotEmpty(cwsGenericProfs) && orderNo != null && orderType != null) {
            orderServiceClient.qcOrder(orderNo, orderType);
        }
    }

    @Transactional(transactionManager = TransactionMangers.DEFAULT, rollbackFor = Exception.class)
    private void insertConveyorAutoShipQueue(Integer orderType, Integer orderNo, String cartonNo, Integer type, String status, Integer locNo, Integer entryId, String plFlag, String overPackFlag, PickToToteRequest request) {
        ConveyorAutoShipQueueId id = new ConveyorAutoShipQueueId();
        id.setOrderNo(orderNo);
        id.setOrderType(orderType);
        id.setCartonNo(cartonNo);

        ConveyorAutoShipQueue queue = new ConveyorAutoShipQueue();
        queue.setId(id);
        queue.setType(type);
        queue.setStatus(status);
        queue.setEntryDate(new Date());
        queue.setEntryId(entryId);
        queue.setLocNo(locNo);
        queue.setPlFlag(plFlag);
        queue.sethVersion(0);
        queue.setOverPackFlag(overPackFlag);

        conveyorAutoShipQueueRepository.save(queue);
    }

    private boolean isNoNeedPrintPL(Integer orderType, Integer orderNo) {
        boolean isNoNeedPrint = false;

        List<OrderParamBase> orderProfileParams = new ArrayList<>();
        OrderInfoParam orderInfoParam = new OrderInfoParam();
        orderInfoParam.setOrderNo(orderNo);
        orderInfoParam.setOrderType(orderType);
        orderProfileParams.add(orderInfoParam);

        OrderHeader orderHeader = null;

        // retrieve order from current table first,if no result found, then from history table
        JsonEntity<List<OrderHeader>> currentOrderHeaderListJsonEntity = orderServiceClient.getCurrentOrderHeaders(orderProfileParams);
        if (currentOrderHeaderListJsonEntity != null && CollectionUtils.isNotEmpty(currentOrderHeaderListJsonEntity.getData())) {
            List<OrderHeader> list = currentOrderHeaderListJsonEntity.getData();
            orderHeader = list.get(0);
        }

        if (orderHeader == null) {
            JsonEntity<List<OrderHeader>> historyOrderHeaderListJsonEntity = orderServiceClient.getHistoryOrderHeaders(orderProfileParams);
            if (historyOrderHeaderListJsonEntity != null && CollectionUtils.isNotEmpty(historyOrderHeaderListJsonEntity.getData())) {
                List<OrderHeader> list = historyOrderHeaderListJsonEntity.getData();
                orderHeader = list.get(0);
            }
        }

        if (orderHeader == null) {
            orderHeader = new OrderHeader();
        }

        String dropShip = StringUtils.trimToEmpty(orderHeader.getDropShip() == null ? "" : orderHeader.getDropShip().toUpperCase());

        if (orderType != null && orderType == 1 && ("D".equals(dropShip) || "Y".equals(dropShip))) {
            Integer custNo = null;

            Integer toAcctNo = orderHeader.getToAcctNo();

            if (toAcctNo != null) {
                JsonEntity<Customer> customerJsonEntity = customerServiceClient.customer(toAcctNo);
                if (customerJsonEntity != null && customerJsonEntity.getData() != null) {
                    Customer customer = customerJsonEntity.getData();
                    if (customer != null) {
                        custNo = customer.getCustNo();
                    }
                }
            }

            if (custNo != null) {
                if (CollectionUtils.isNotEmpty(cwsGenericProfRepository.findByProfileTypeAndProfileI("NO_PACKING_LIST", custNo))) {
                    isNoNeedPrint = true;
                }
            }
        }

        return isNoNeedPrint;
    }

    // test data
    // {call cws_conveyor_pick_atzone(7,114,'7V20114H','01','T1030','T1030',4692211,1,'N','N','N','Y')}
    private void doConveyorPick(Integer locNo, int userId, String binLoc, String zone, String toteId, String nonRepackableCartonNo, Integer skuNo, int pickQty, String forceNonRepackable, String needQcZone) {
        String procedure = "{?=call cws_conveyor_pick_atzone(?,?,?,?,?,?,?,?,?,?,?,?)}";
        // No need enhance this, because no transaction.
        // simonc, markl reviewd.
        Integer result = jdbcTemplate.execute(procedure, (CallableStatementCallback<Integer>) cs -> {
            cs.registerOutParameter(1, Types.INTEGER);
            cs.setInt(2, locNo);
            cs.setInt(3, userId);
            cs.setString(4, binLoc);
            cs.setString(5, zone);
            cs.setString(6, toteId);
            cs.setString(7, nonRepackableCartonNo);
            cs.setInt(8, skuNo);
            cs.setInt(9, pickQty);
            cs.setString(10, "N");
            cs.setString(11, "N");
            cs.setString(12, forceNonRepackable);
            cs.setString(13, needQcZone);
            cs.execute();
            return cs.getInt(1);
        });
        LOG.info("call sp cws_conveyor_pick_atzone return ---> " + result);
    }

    /**
     * this.insertCartonHeader(orderType, orderNo, toteId, CwsConstants.BYPASS_QC_PACKLANE, uWeight * pickQty + tareWeight, tareWeight, boxId, userId, locNo);
     */

    @Transactional(transactionManager = TransactionMangers.DEFAULT, rollbackFor = Exception.class)
    private void insertCartonHeader(Integer orderType, Integer orderNo, String cartonNo, String packLane, double calcWgt, double boxWght, String boxId, Integer entryId, Integer locNo) {
        CartonHeader cartonHeader = new CartonHeader();

        cartonHeader.setOrderType(orderType);
        cartonHeader.setOrderNo(orderNo);
        cartonHeader.setPackLane(packLane);
        cartonHeader.setCartonNo(cartonNo);
        cartonHeader.setCalcWgt(calcWgt);
        cartonHeader.setShipWgt(calcWgt);
        cartonHeader.setActualWgt(calcWgt);
        cartonHeader.setTareWgt(boxWght);
        cartonHeader.setBoxId(boxId);
        cartonHeader.setCrUid(entryId);
        cartonHeader.setCrDttm(new Date());
        cartonHeader.setCartonLocNo(locNo);

        cartonHeaderRepository.save(cartonHeader);
    }

    @Transactional
    private void removeToteFromConveyor(int locNo, String toteId, int orderType, int orderNo) {
        CwsConveyorSocket sk = new CwsConveyorSocket();
        sk.setCartonId(toteId);
        sk.setOrderNo(orderNo);
        sk.setOrderType(orderType);
        sk.setLocNo(locNo);
        sk.setZoneList("");
        sk.setStatus("I");
        sk.setEntryDate(new Date());
        cwsConveyorSocketRepository.save(sk);
    }

    @Transactional(transactionManager = TransactionMangers.DEFAULT)
    private void insertConveyorSocketByShipQueue(final int locNo, final int orderType, final int orderNo) {

        List<CwsGenericProf> profList = cwsGenericProfRepository.findByProfileType("PSI_AutoPick_Flag");
        if (CollectionUtils.isNotEmpty(profList) && "Y".equals(profList.get(0).getProfileC())) {
            return;
        }

        List<ConveyorAutoShipQueue> shipQueueList = conveyorAutoShipQueueRepository.findByIdOrderNoAndIdOrderType(orderNo, orderType);
        if (CollectionUtils.isNotEmpty(shipQueueList)) {
            for (ConveyorAutoShipQueue shipQueue : shipQueueList) {
                String zoneList = "";

                if ("Y".equals(shipQueue.getOverPackFlag())) {
                    zoneList += " " + CwsConstants.TAPE_ZONE;
                }

                if ("Y".equals(shipQueue.getPlFlag())) {
                    zoneList += " " + CwsConstants.PL_PRINT_ZONE;
                }

                zoneList += " " + CwsConstants.LABEL_ZONE;

                if (zoneList.trim().length() != 0) {
                    this.insertConveyorSocket(locNo, shipQueue.getId().getCartonNo(), orderType, orderNo, zoneList.trim());
                }
            }
        }
    }

    private void insertConveyorSocket(final int locNo, final String cartonId, final int orderType,
                                      final int orderNo, final String zoneList) {
        CwsConveyorSocket conveyorSocket = new CwsConveyorSocket();

        conveyorSocket.setLocNo(locNo);
        conveyorSocket.setCartonId(cartonId);
        conveyorSocket.setOrderType(orderType);
        conveyorSocket.setOrderNo(orderNo);
        conveyorSocket.setZoneList(zoneList);
        conveyorSocket.setEntryDate(new Date());
        conveyorSocket.setStatus("I");

        cwsConveyorSocketRepository.save(conveyorSocket);
    }

    private boolean isPickComplete(final Integer orderType, final Integer orderNo) {
        final String sql = "{?=call cws_generic_rf_by_order(?,?,?)}";
        // No need enhance this, because no transaction.
        // simonc, markl reviewd.
        Integer returnCode = jdbcTemplate.execute(sql, (CallableStatementCallback<Integer>) cs -> {
            cs.registerOutParameter(1, Types.INTEGER);
            cs.setInt(2, orderType);
            cs.setInt(3, orderNo);
            cs.setString(4, "CHECK_PICK_COMPLETE");
            cs.execute();
            return cs.getInt(1);
        });
        return returnCode != null && returnCode == 1;
    }


    @Transactional(transactionManager = TransactionMangers.DEFAULT, rollbackFor = Exception.class)
    public void sendToZone(final int locNo, final String toteId, final OrderKey orderKey, final String zone,
                           final boolean zoneFinishPick) {
        if (zoneFinishPick) {
            Set<String> profileTypes = new HashSet<>();
            profileTypes.add("ZONE_PICK_DONE");

            List<WhProfile> profiles = whProfileRepository.getByLocNoAndProfileTypes(locNo, profileTypes);
            if (CollectionUtils.isEmpty(profiles)) {
                return;
            }
        }
        int orderNo = orderKey.getOrderNo();
        int orderType = orderKey.getOrderType();
        String status = zoneFinishPick ? "Z" : "I";

        CwsConveyorSocket skt = new CwsConveyorSocket();
        skt.setCartonId(toteId);
        skt.setOrderNo(orderNo);
        skt.setOrderType(orderType);
        skt.setLocNo(locNo);
        skt.setZoneList(zone);
        skt.setStatus(status);
        skt.setEntryDate(new Date());

        cwsConveyorSocketRepository.save(skt);
    }

    @Transactional
    private void insertCwsBinMngLog(final PickToToteInnerRequest request) {
        cwsBinMngLogService.insertLog(BinMgmtLog.WRONG_REPACKABLE, request.getCurrentUserId(), request.getLocNo(), request.getBinLoc(), request.getSkuNo(), request.getPickQty(),
                request.getOrderNo(), request.getOrderType(), null, request.getInvType(), null, true);
    }

    /**
     * Check carton# format
     */
    private void checkCartonNo(final String cartonNo) {
        // Picker scan the tote# ,maybe non-repackable flag is wrong.
        if (cartonNo != null && cartonNo.startsWith("C") && cartonNo.length() == 12 && CwsStringUtil.isDigits(cartonNo.substring(1))) {
            throw new BizException("Are you sure it is non-repackable ?");
        }
    }

    private void throwBizExceptionOfCheckCartonNoWhetherUsed() {
        throw new BizException("This carton# has already been used.");
    }

    /**
     * Whether a carton is used from conveyor point of view.
     */
    @Override
    public boolean checkCartonNoWhetherUsed(Integer locNo, String cartonNo) {

        // If Carton# exists in any one of below 3 tables, then throw exception "This carton# has already been used."
        boolean existsByCartonNo = cartonHeaderRepository.existsByCartonNo(cartonNo) != null;

        if (existsByCartonNo) {
            throwBizExceptionOfCheckCartonNoWhetherUsed();
        }

        boolean existsByCartonNoTwo = cartonInPackingRepository.existsByCartonNo(cartonNo) != null;

        if (existsByCartonNoTwo) {
            throwBizExceptionOfCheckCartonNoWhetherUsed();
        }

        List<Integer> cartonUsedList = cwsConveyorPickRepository.isCartonUsed(locNo, cartonNo);
        if (CollectionUtils.isNotEmpty(cartonUsedList)) {
            throwBizExceptionOfCheckCartonNoWhetherUsed();
        }
        return true;
    }

    @Override
    public List<AlterBinResponse> showAvailableBin(Integer locNo, String zone, Integer skuNo, Integer invType) {
        List<Map<String, Object>> list = cwsInvRepository.showAvailableBin(locNo, zone, skuNo, invType);
        List<AlterBinResponse> returnList = new ArrayList<>(list.size());
        if (CollectionUtils.isNotEmpty(list)) {
            for (Map<String, Object> resultMap : list) {
                if (resultMap != null && resultMap.size() == 3) {
                    String binLoc = resultMap.get("binLoc") != null ? (String) resultMap.get("binLoc") : null;
                    Integer binQty = resultMap.get("binQty") != null ? (Integer) resultMap.get("binQty") : null;
                    String discrepFlag = resultMap.get("discrepFlag") != null ? (String) resultMap.get("discrepFlag") : null;
                    AlterBinResponse alterBinResponse = new AlterBinResponse();
                    alterBinResponse.setBinLoc(binLoc);
                    alterBinResponse.setTotQty(binQty);
                    alterBinResponse.setDiscrep("Y".equals(discrepFlag));
                    returnList.add(alterBinResponse);
                }
            }
        }
        return returnList;
    }

    @Override
    @Transactional(transactionManager = TransactionMangers.DEFAULT, rollbackFor = Exception.class)
    public boolean newTote(String zone, Integer locNo, String curToteId, String newToteId, Boolean bypassQC) {
        // 1) Validate if input tote is in use.
        boolean isToteInUse = conveyorPutawayInjectService.isToteInUse(locNo, newToteId);

        if (isToteInUse) {
            throw new BizException("Tote# is in used.");
        }

        // 2) Move tote to history
        cwsConveyorPickHistoryRepository.insertData(locNo, newToteId);
        cwsConveyorPickRepository.deleteByLocNoAndCartonId(newToteId, locNo);

        // 3) Call SP to create new tote#
        String needQcZone = bypassQC ? "N" : "Y";
        // no truncate, no select into statement, no problem.
        // simonc, markl reviewd.
        String sql = "{call cws_conveyor_pick_new_tote(?,?,?,?,?,?)}";
        jdbcTemplate.execute(sql, (CallableStatementCallback<?>) cs -> {
            cs.setInt(1, locNo);
            cs.setString(2, curToteId);
            cs.setString(3, newToteId);
            cs.setString(4, zone);
            cs.setString(5, "N");
            cs.setString(6, needQcZone);
            cs.execute();
            return null;
        });
        return true;
    }

    @Override
    @Transactional(transactionManager = TransactionMangers.DEFAULT, rollbackFor = Exception.class)
    public BoxSize validateBoxId(BoxValidationRequest request, UserInfo userInfo) {

        BoxSize boxSize = new BoxSize();

        String oldBoxId = request.getOldBoxId();
        String newBoxId = request.getBoxId();
        String toteId = request.getToteId();
        Integer locNo = request.getLocNo();
        String cartonNo = request.getCartonNo();

        Integer orderNo = null;
        Integer orderType = null;
        Integer skuNo = null;

        List<Map<String, Object>> orderList = cwsConveyorPickRepository.getOrderNoAndOrderTypeByCartonIdAndLocNo(toteId, locNo);
        if (CollectionUtils.isNotEmpty(orderList)) {
            Map<String, Object> orderMap = orderList.get(0);
            if (orderMap != null) {
                orderNo = (Integer) orderMap.get("orderNo");
                orderType = (Integer) orderMap.get("orderNo");
                skuNo = (Integer) orderMap.get("skuNo");
            }
        }

        CwsCartonProf newRightBox = this.getRightBoxById(newBoxId, locNo);

        if (newRightBox == null) {
            throw new BizException(String.format("Input boxId: %s can't be found in system!", newBoxId));
        }

        // if (newRightBox.getId() != null && newRightBox.getId().getBoxId() != null) {
        //    if (CwsStringUtil.toString(newRightBox.getId().getBoxId()).equals(oldBoxId)) {
        //        throw new BizException("The new Box is same with old one !");
        //    }
        // }

        // save new box into system(cws_smartpick_header), overwrite the old one
        cwsSmartpickHeaderRepository.updateBoxId(orderNo, orderType, newBoxId);

        try {
            CwsEventLog cwsEventLog = new CwsEventLog();
            cwsEventLog.setEventType(CwsEventLogEventType.CHANGE_BOX_EVENT);
            cwsEventLog.setSourceBin(cartonNo);
            cwsEventLog.setSourceId(oldBoxId);
            cwsEventLog.setDestId(newBoxId);
            cwsEventLog.setOrderNo(orderNo);
            cwsEventLog.setOrderType(orderType);
            cwsEventLog.setSkuNo(skuNo);
            commonService.saveCwsEventLog(cwsEventLog, userInfo);
        } catch (Exception e) {
            LOG.error("Save cws event log failed, event type is CHANGE_BOX_EVENT !");
        }

        // Box box = conveyorPaperPickValidate.getBox(toteId, locNo, userInfo);
        // boxSize.setBoxId(box.getBoxId());
        // boxSize.setWidth(box.getWidth() / 100);
        // boxSize.setHeight(box.getHeight() / 100);
        // boxSize.setLength(box.getLength() / 100);

        String boxId = newRightBox.getId().getBoxId();
        double newLength = newRightBox.getLength().doubleValue() * 100;
        double newWidth = newRightBox.getWidth().doubleValue() * 100;
        double newHeight = newRightBox.getHeight().doubleValue() * 100;

        boxSize.setBoxId(boxId);
        boxSize.setHeight((int) (newHeight / 100));
        boxSize.setLength((int) (newLength / 100));
        boxSize.setWidth((int) (newWidth / 100));

        return boxSize;
    }

    @Override
    // @Transactional(transactionManager = TransactionMangers.DEFAULT, rollbackFor = Exception.class)
    public Map<String, Object> doPick(ConveyorMultiPaperlessDoPickRequest request,UserInfo userInfo) {

        Map<String, Object> returnMap = new HashMap<>(2);

        returnMap.put("currentZonePickDone", "false"); //  carton pick done in current zone
        returnMap.put("leftQtyInBin", "0");

        Boolean bypassQC = request.getBypassQC();
        String binLoc = request.getBinLoc();
        String boxId = request.getBoxId();
        String cartonNo = request.getCartonNo();
        Integer locNo = request.getLocNo();
        String zone = request.getZone();
        Integer skuNo = request.getSkuNo();
        List<String> snList = request.getSnList();
        Integer pickQty = request.getPickQty();

        Integer userId = userInfo.getUserid();
        Integer orderNo = null;
        Integer orderType = null;

        TotePickHeader totePickHeader = this.getTotePickHeader(locNo, cartonNo, zone);
        totePickHeader = (totePickHeader == null ? new TotePickHeader() : totePickHeader);

        if (CollectionUtils.isEmpty(totePickHeader.getDetails())) {
            throw new BizException("Pick details not found!");
        }

        totePickHeader.setBypassQcCandidateOrder(bypassQC);
        totePickHeader.setOrderAllCanBypassQC(bypassQC);

        TotePickDetail currentTotePickDetail = null;

        List<TotePickDetail> totePickDetailList = totePickHeader.getDetails();

        if (CollectionUtils.isNotEmpty(totePickDetailList)) {
            for (TotePickDetail totePickDetail : totePickDetailList) {
                if (totePickDetail.getSkuNo().intValue() == skuNo.intValue()) {
                    currentTotePickDetail = totePickDetail;
                    break;
                }
            }
        }

        if (currentTotePickDetail != null) {
            currentTotePickDetail.setBypassQC(bypassQC);
        }

        // currentTotePickDetail = (currentTotePickDetail == null ? new TotePickDetail() : currentTotePickDetail);

        List<Map<String, Object>> orderMapList = cwsConveyorPickRepository.getOrderNoAndOrderTypeByCartonIdAndLocNo(cartonNo, locNo);
        if (CollectionUtils.isNotEmpty(orderMapList)) {
            Map<String, Object> orderMap = orderMapList.get(0);
            orderNo = (Integer) orderMap.get("orderNo");
            orderType = (Integer) orderMap.get("orderType");
        }

        orderNo = totePickHeader.getOrderKey().getOrderNo();
        orderType = totePickHeader.getOrderKey().getOrderType();
        Integer orderLineNo = currentTotePickDetail != null ? currentTotePickDetail.getOrderLineNo() : null;
        skuNo = currentTotePickDetail != null ? currentTotePickDetail.getSkuNo() : null;

        if (currentTotePickDetail != null && currentTotePickDetail.isNeedScanSN() && CollectionUtils.isNotEmpty(snList)) {
            this.saveScannedSns(orderType, orderNo, orderLineNo, cartonNo, skuNo, snList,
                    currentTotePickDetail.getInputSerialIMEI(), currentTotePickDetail.getInputSerialESN(), currentTotePickDetail.getInputSerialICCID(),
                    currentTotePickDetail.getInputSerialMAC(), userId, locNo);
        }

        String needQcZone = "";
        if (currentTotePickDetail != null) {
            needQcZone = currentTotePickDetail.isBypassQC() ? "N" : "Y";
        }
        needQcZone = bypassQC ? "N" : "Y";
        String forceNonRepackable = "N";
        this.doConveyorPick(locNo, userId, binLoc, zone, cartonNo, cartonNo, skuNo, pickQty, forceNonRepackable, needQcZone);


        // order finished pick in current zone, send zone finish pick socket
        OrderKey orderKey = new OrderKey();
        orderKey.setOrderNo(orderNo);
        orderKey.setOrderType(orderType);

        this.sendToZone(locNo, cartonNo, orderKey, zone, true);

        CwsSmartpickDetailId cwsSmartpickDetailId = new CwsSmartpickDetailId();
        cwsSmartpickDetailId.setOrderNo(orderNo);
        cwsSmartpickDetailId.setOrderType(orderType);
        cwsSmartpickDetailId.setSkuNo(skuNo);
        cwsSmartpickDetailId.setBoxSeq(1);
        Optional<CwsSmartpickDetail> optionalCwsSmartpickDetail = cwsSmartpickDetailRepository.findById(cwsSmartpickDetailId);
        if (optionalCwsSmartpickDetail.isPresent()) {
            CwsSmartpickDetail cwsSmartpickDetail = optionalCwsSmartpickDetail.get();
            cwsSmartpickDetail.setPickDate(new Date());
            if (cwsSmartpickDetail.getPickQty() == null) {
                cwsSmartpickDetail.setPickQty(0);
            }
            cwsSmartpickDetail.setPickToCarton(cartonNo);

            if (cwsSmartpickDetail.getPickQty() + pickQty >= cwsSmartpickDetail.getOrderQty()) {
                cwsSmartpickDetail.setPickQty(cwsSmartpickDetail.getOrderQty());
            }

            if (cwsSmartpickDetail.getPickQty().equals(cwsSmartpickDetail.getOrderQty())) {
                cwsSmartpickDetail.setPickStatus(CwsSmartpickStatus.COMPLETED);
            }

            if (cwsSmartpickDetail.getPickerId() == null) {
                cwsSmartpickDetail.setPickerId(userId);
            }
            cwsSmartpickDetailRepository.save(cwsSmartpickDetail);
        }


        Long unFinishedCount = cwsConveyorPickRepository.getUnFinishedCount(orderNo, orderType, zone);

        boolean existsUnFinished = unFinishedCount != null && unFinishedCount > 0;

        if (!existsUnFinished) {
            String msg = "This order pick is finished at this zone!";
            LOG.info(msg);

            // order finished pick in current zone, send zone finish pick socket
            // after a zone pick done, add zone# into socket table with status Z
            // An order may be picked at several zones,need to check the whole order is picked completely.
            if (this.isPickComplete(orderType, orderNo)) {
                cwsSmartpickHeaderRepository.finishMultiPaperlessPickLine(orderNo, orderType, bypassQC ? "BYPASSQC" : "Conveyor");

                if (bypassQC) {
                    double orderWeight = 0;
                    List<OrderDetail> orderDetailList = orderService.getOrderDetailList(orderType, orderNo);
                    if (CollectionUtils.isNotEmpty(orderDetailList)) {
                        List<OrderDetail> orderDetails = conveyorPickInjectService.getFilterOrderDetails(orderDetailList);
                        if (CollectionUtils.isNotEmpty(orderDetailList)) {
                            for (OrderDetail orderDetail : orderDetails) {
                                double uWeight = 0;
                                Integer skuNo1 = orderDetail.getSkuNo();
                                JsonEntity<PartMaster> partMasterJsonEntity = partServiceClient.getPartMasterBySkuNo(skuNo1);
                                if (partMasterJsonEntity != null) {
                                    PartMaster part = partMasterJsonEntity.getData();
                                    if (part != null && part.getWeight() != null) {
                                        uWeight = part.getWeight().doubleValue();
                                    }
                                    orderWeight += uWeight * orderDetail.getOrderQty();
                                }
                                conveyorPickCommonService.insertCartonDetail(cartonNo, orderDetail.getId().getOrderLineNo(), skuNo1, orderDetail.getOrderQty(), userId);
                            }
                        }
                    }

                    double tareWeight = 0;
                    CwsCartonProf cwsCartonProf = this.getRightBoxById(boxId, locNo);
                    if (cwsCartonProf != null && cwsCartonProf.getTareWeight() != null) {
                        tareWeight = cwsCartonProf.getTareWeight().doubleValue();
                    }

                    conveyorPickCommonService.insertCartonHeader(orderType, orderNo, cartonNo, CwsConstants.BYPASS_QC_PACKLANE, orderWeight + tareWeight, tareWeight, boxId, userId, locNo);

                    // IF CUST# IN NO_PACKING_LIST,drop ship orders (oh.drop_ship = D/Y), order_type =1, No packing list needed
                    boolean isNoNeedPrintPL = this.isNoNeedPrintPL(orderType, orderNo);
                    String plFlag = isNoNeedPrintPL ? null : "Y";
                    String packFlag = "Y";
                    conveyorPickCommonService.insertConveyorAutoShipQueue(orderType, orderNo, cartonNo, CwsConstants.TYPE_REGULAR, CwsConstants.ORDER_READY_GENERATE_LABEL_DATA, locNo, userId, plFlag, packFlag);

                    // send order carton to tape zone(110)/pl print zone(120)/label zone(130).
                    // simon requested.
                    // this.insertConveyorSocketByShipQueue(locNo, orderType, orderNo);

                    conveyorPickCommonService.updateBoxSeq(orderNo, orderType);
                    conveyorPickCommonService.updateQCDateWithSomeCondition(orderNo, orderType);

                    msg = "Order pick complete.";
                    LOG.info(msg);
                }
            }

            returnMap.put("currentZonePickDone", true);

        }

        CwsInv cwsInv = cwsInvRepository.findByIdLocNoAndIdBinLocAndIdSkuNo(request.getLocNo(), request.getBinLoc(), request.getSkuNo());
        if (cwsInv != null) {
            returnMap.put("leftQtyInBin", cwsInv.getTotQty());
        }

        return returnMap;
    }
}