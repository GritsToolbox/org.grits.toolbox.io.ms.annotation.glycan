package org.grits.toolbox.io.ms.annotation.glycan.process.export;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.eurocarbdb.resourcesdb.Config;
import org.eurocarbdb.resourcesdb.io.MonosaccharideConversion;
import org.eurocarbdb.resourcesdb.io.MonosaccharideConverter;
import org.glycomedb.residuetranslator.ResidueTranslator;
import org.grits.toolbox.io.ms.annotation.process.export.MSAnnotationExcelListener;
import org.grits.toolbox.io.ms.annotation.process.export.MSAnnotationWriterExcel;
import org.grits.toolbox.utils.process.GlycoWorkbenchUtil;
import org.grits.toolbox.widgets.processDialog.ProgressDialog;
import org.grits.toolbox.widgets.progress.IProgressThreadHandler;

public class MSGlycanAnnotationSummaryExportProcess extends MSGlycanAnnotationExportProcess {
	//log4J Logger
	private static final Logger logger = Logger.getLogger(MSGlycanAnnotationSummaryExportProcess.class);
	
	@Override
	protected MSAnnotationWriterExcel getNewMSAnnotationWriterExcel() {
		return new MSGlycanAnnotationSummaryWriterExcel();
	}
	
	@Override
	public boolean threadStart(IProgressThreadHandler a_progressThreadHandler) throws Exception {
		try{
			// write values to Excel
			MSAnnotationWriterExcel t_writerExcel = getNewMSAnnotationWriterExcel();

			t_writerExcel.createNewFile(getOutputFile(), getTableDataObject(), getLastVisibleColInx(),
					new MSAnnotationExcelListener((ProgressDialog) a_progressThreadHandler));	       
			
			Config t_objConf = new Config();
			MonosaccharideConversion t_msdb = new MonosaccharideConverter(t_objConf);
			try {
				m_gwbUtil = new GlycoWorkbenchUtil(new ResidueTranslator(), t_msdb);
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
			}
			
			((ProgressDialog) a_progressThreadHandler).setMax(this.tableDataObject.getTableData().size());
			((ProgressDialog) a_progressThreadHandler).setProcessMessageLabel("Processing data");
			
			for( int i = 0; i < getTableDataObject().getTableData().size(); i++ )  {
				if(isCanceled()) {
					t_writerExcel.close();
					return false;
				}
				t_writerExcel.writeRow(i, false);
				//show in dialog
				((ProgressDialog) a_progressThreadHandler).updateProgresBar("Row: "+ (i+1));
			}
			t_writerExcel.close();
		} catch (Exception e) {
			logger.error("Failed to create the Excel file", e);
			throw e;	
		}
		return true;
	}
}
