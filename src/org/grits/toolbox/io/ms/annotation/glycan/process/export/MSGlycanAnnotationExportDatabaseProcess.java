package org.grits.toolbox.io.ms.annotation.glycan.process.export;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.apache.log4j.Logger;
import org.grits.toolbox.datamodel.ms.annotation.tablemodel.MSAnnotationTableDataObject;
import org.grits.toolbox.io.ms.annotation.process.export.MSAnnotationExportDatabaseProcess;
import org.grits.toolbox.ms.om.data.Feature;
import org.grits.toolbox.util.structure.glycan.database.GlycanDatabase;
import org.grits.toolbox.util.structure.glycan.database.GlycanStructure;
import org.grits.toolbox.utils.image.GlycanImageProvider;
import org.grits.toolbox.widgets.processDialog.ProgressDialog;
import org.grits.toolbox.widgets.progress.IProgressThreadHandler;

public class MSGlycanAnnotationExportDatabaseProcess extends MSAnnotationExportDatabaseProcess {
	//log4J Logger
	private static final Logger logger = Logger.getLogger(MSGlycanAnnotationExportDatabaseProcess.class);
		
	@Override
	public boolean threadStart(IProgressThreadHandler a_progressThreadHandler) throws Exception {
		try{
			
			// copy the information from the user into the new database metadata
	        GlycanDatabase database = new GlycanDatabase();
	        database.setName(getDbName());
	        database.setDescription(getDescription());
	        database.setVersion(getVersion());
	        
	        List<GlycanStructure> structures  = new ArrayList<GlycanStructure>();
	        database.setStructures(structures);
			
			((ProgressDialog) a_progressThreadHandler).setMax(this.tableDataObject.getTableData().size());
			((ProgressDialog) a_progressThreadHandler).setProcessMessageLabel("Exporting data");
			int cnt = 1;
			for( int i = 0; i < getTableDataObject().getTableData().size(); i++ )  {
				if(isCanceled()) {
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
				String iRowId = Feature.getRowId(iPeakId, iScanNum, ((MSAnnotationTableDataObject) getTableDataObject()).getUsesComplexRowId());
				if(  iPeakId != null && sFeatureId != null && iParentScanNum != null &&
						getTableDataObject().isHiddenRow(iParentScanNum, iRowId, sFeatureId) ) 
					continue;
				
				boolean bInvisible = false;
				if( iPeakId != null && iParentScanNum != null && getTableDataObject().isInvisibleRow(iParentScanNum, iRowId) )
					bInvisible = true;
					
				if (!bInvisible) {
					int sequenceCol = getTableDataObject().getSequenceCols().get(0);
					String sequence = (String) getTableDataObject().getTableData().get(i).getDataRow().get(sequenceCol);
					if (sequence != null && !sequence.isEmpty()) {
						int iInx1 = sequence.indexOf(GlycanImageProvider.COMBO_SEQUENCE_SEPARATOR);
						if (iInx1 < 0) {  // single sequence
							GlycanStructure structure = new GlycanStructure();
							structure.setGWBSequence(sequence);
							structure.setId(cnt + "");
							cnt++;
							structures.add(structure);
						} else {
							String sRemaining = sequence;
							do {
								String sSeq = iInx1 > 0 ? sRemaining.substring(0, iInx1) : sRemaining;
								GlycanStructure structure = new GlycanStructure();
								structure.setGWBSequence(sSeq);
								structure.setId(cnt + "");
								cnt++;
								structures.add(structure);
								sRemaining = iInx1 > 0 ? sRemaining.substring(iInx1	+ GlycanImageProvider.COMBO_SEQUENCE_SEPARATOR.length()) : null;
								iInx1 = sRemaining != null ? sRemaining.indexOf(GlycanImageProvider.COMBO_SEQUENCE_SEPARATOR) : -1;
							} while (sRemaining != null);
						}
					}
					
				}
				//show in dialog
				((ProgressDialog) a_progressThreadHandler).updateProgresBar("Scan: "+ (i+1));
			}
			database.setStructureCount(structures.size());
			((ProgressDialog) a_progressThreadHandler).setMax(database.getStructureCount());
			((ProgressDialog) a_progressThreadHandler).setProcessMessageLabel("Exporting data");
			saveDatabase(database, getOutputFile());
		}catch(Exception e)
		{
			logger.error(e.getMessage(), e);
			throw e;
		}
		return true;
	}
	
	private void saveDatabase(GlycanDatabase database, String filePath) throws JAXBException
    {
        // create JAXB context and marshaller
        JAXBContext t_context = JAXBContext.newInstance(GlycanDatabase.class);
        Marshaller t_marshaller = t_context.createMarshaller();
        t_context = JAXBContext.newInstance(GlycanDatabase.class);
        t_marshaller = t_context.createMarshaller();
        // output pretty printed
        t_marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        // write the file
        t_marshaller.marshal(database, new File(filePath));
    }
}
