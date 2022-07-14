package org.grits.toolbox.io.ms.annotation.glycan.process.export;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.eurocarbdb.application.glycanbuilder.Glycan;
import org.eurocarbdb.application.glycanbuilder.ResidueType;
import org.eurocarbdb.resourcesdb.Config;
import org.eurocarbdb.resourcesdb.io.MonosaccharideConversion;
import org.eurocarbdb.resourcesdb.io.MonosaccharideConverter;
import org.glycomedb.residuetranslator.ResidueTranslator;
import org.grits.toolbox.datamodel.ms.annotation.tablemodel.MSAnnotationTableDataObject;
import org.grits.toolbox.io.ms.annotation.process.export.MSAnnotationExportByonicProcess;
import org.grits.toolbox.ms.om.data.Feature;
import org.grits.toolbox.util.structure.glycan.count.GlycanCompositionCountOperator;
import org.grits.toolbox.util.structure.glycan.count.SearchQueryItem;
import org.grits.toolbox.utils.image.GlycanImageProvider;
import org.grits.toolbox.utils.process.GlycoWorkbenchUtil;
import org.grits.toolbox.widgets.processDialog.ProgressDialog;
import org.grits.toolbox.widgets.progress.IProgressThreadHandler;

public class MSGlycanAnnotationExportByonicProcess extends MSAnnotationExportByonicProcess {
	
	//log4J Logger
	private static final Logger logger = Logger.getLogger(MSGlycanAnnotationExportByonicProcess.class);
	
	protected GlycoWorkbenchUtil m_gwbUtil;
	List<SearchQueryItem> componentList;
	
	class Row {
		Map<String, Integer> componentCounts;
		Double mass;
		String featureId;
		
		public Row(String featureId, Double mass, Map<String, Integer> counts) {
			this.featureId = featureId;
			this.mass = mass;
			this.componentCounts = counts;
		}
		
		public void setComponentCounts(Map<String, Integer> componentCounts) {
			this.componentCounts = componentCounts;
		}
		
		public void setMass(Double mass) {
			this.mass = mass;
		}
		
		public void setFeatureId(String featureId) {
			this.featureId = featureId;
		}
		
		@Override
		public String toString() {
			if (componentCounts == null)
				return null;
			String output = "";
			for (String component : componentCounts.keySet()) {
				int count = componentCounts.get(component);
				if (count == 0)  // no need to report
					continue;
				output += component +"("+ componentCounts.get(component) + ")";
			}
			output += " % " + mass;
			return output;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (this == obj) return true;
			if (!(obj instanceof Row)) return false;
			return featureId.equals(((Row)obj).featureId);
		}
		
		@Override
		public int hashCode() {
			return featureId.hashCode();
		}
	}
	
	public void setComponentList(List<SearchQueryItem> componentList) {
		this.componentList = componentList;
	}

	@Override
	public boolean threadStart(IProgressThreadHandler a_progressThreadHandler) throws Exception {
		try{
			BufferedWriter out = new BufferedWriter(new FileWriter(getOutputFile()));
			out.write("% Created by GRITS on " + new Date());
			out.newLine();
			
			Config t_objConf = new Config();
			MonosaccharideConversion t_msdb = new MonosaccharideConverter(t_objConf);
			try {
				m_gwbUtil = new GlycoWorkbenchUtil(new ResidueTranslator(), t_msdb);
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
			}
			
			((ProgressDialog) a_progressThreadHandler).setMax(this.tableDataObject.getTableData().size());
			((ProgressDialog) a_progressThreadHandler).setProcessMessageLabel("Exporting data");
			Set<Row> rowsToWrite = new HashSet<>();
			for( int i = 0; i < getTableDataObject().getTableData().size(); i++ )  {
				if(isCanceled()) {
					out.close();
					return false;
				}
				
				int iParentScanIdCol = -1;
				if( getTableDataObject().getParentNoCol() != null && ! getTableDataObject().getParentNoCol().isEmpty() ) {
					iParentScanIdCol = getTableDataObject().getParentNoCol().get(0);
				}
				int iPeakIdCol = getTableDataObject().getPeakIdCols().get(0);
				int iFeatureIdCol = getTableDataObject().getFeatureIdCols().get(0);
				Integer iParentScanNum = null;
				if( getMasterParentScan() != -1 ) {
					iParentScanNum = getMasterParentScan();
				} else if ( iParentScanIdCol != -1 && getTableDataObject().getTableData().get(i).getDataRow().get(iParentScanIdCol) != null ) {
					iParentScanNum = (Integer) getTableDataObject().getTableData().get(i).getDataRow().get(iParentScanIdCol);
				}
				Integer iPeakId = (Integer) getTableDataObject().getTableData().get(i).getDataRow().get(iPeakIdCol);
				String sFeatureId = (String) getTableDataObject().getTableData().get(i).getDataRow().get(iFeatureIdCol);
				if( sFeatureId == null && hideUnAnnotatedRows() ) {
					continue;
				}
				Integer iScanNum = null;
				if( getTableDataObject().getScanNoCols() != null && ! getTableDataObject().getScanNoCols().isEmpty() ) {
					iScanNum = getTableDataObject().getScanNoCols().get(0);
				}
				String iRowId = Feature.getRowId(iPeakId, iScanNum, ((MSAnnotationTableDataObject) getTableDataObject()).getUsesComplexRowId() );
				if(  iPeakId != null && sFeatureId != null && iParentScanNum != null &&
						getTableDataObject().isHiddenRow(iParentScanNum, iRowId, sFeatureId) ) 
					continue;
				
				boolean bInvisible = false;
				if( iPeakId != null && iParentScanNum != null && getTableDataObject().isInvisibleRow(iParentScanNum, iRowId) )
					bInvisible = true;
					
				if (!bInvisible) {
					// generate composition counts
					int sequenceCol = getTableDataObject().getSequenceCols().get(0);
					String sequence = (String) getTableDataObject().getTableData().get(i).getDataRow().get(sequenceCol);
					if (sequence != null && !sequence.isEmpty()) {
						int iInx1 = sequence.indexOf(GlycanImageProvider.COMBO_SEQUENCE_SEPARATOR);
						if (iInx1 < 0) {  // single sequence
							m_gwbUtil.parseGWSSequence(sequence);
							Glycan glycan = m_gwbUtil.getGlycoWorkbenchGlycan();
							glycan.setReducingEndType(ResidueType.createFreeReducingEnd());
							LinkedHashMap<String, Integer> counts = new LinkedHashMap<>();
							boolean matched = GlycanCompositionCountOperator.matchAndCountComposition(glycan.toSugar(), componentList, counts);
							if (matched) rowsToWrite.add(new Row(sFeatureId, glycan.computeMass(), counts));  // should remove duplicates since this is a 
							else {
								logger.info("Not a match, Excluding: " + sFeatureId);
								logger.info("Sequence: " + sequence);
							}
						} else {
							// cannot handle this case
						}
					}
					
				}
				//show in dialog
				((ProgressDialog) a_progressThreadHandler).updateProgresBar("Scan: "+ (i+1));
			}
			
			((ProgressDialog) a_progressThreadHandler).setMax(rowsToWrite.size());
			((ProgressDialog) a_progressThreadHandler).setProcessMessageLabel("Exporting data");
			int i=0;
			for (Row row : rowsToWrite) {
				out.write(row.toString());
				out.newLine();
				logger.debug(row.toString() + " -->" + row.featureId);
				//show in dialog
				((ProgressDialog) a_progressThreadHandler).updateProgresBar("Scan: "+ (++i));
			}
			
			out.close();
		}catch(Exception e)
		{
			logger.error(e.getMessage(), e);
			throw e;
		}
		return true;
	}
}
