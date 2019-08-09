package test;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

//import database.DataBaseConnector;
import util.CfLoginUtils;


public class CFLogin {
	String predixUserName;
	String predixPassword;
	@BeforeTest
	  @Parameters({"app_url"})
	  public  void setUp(String app_url) throws Exception {
		CfLoginUtils.setUp(app_url);
	}
	 
	
	  @Test(alwaysRun=true)
	  public void waitForLoginPage() throws Exception{
		  CfLoginUtils.waitForLoginPage();
		  }
	  
	    @Test(dependsOnMethods="waitForLoginPage")
	  @Parameters("username")
	  public void enterLoginCredentials(String username) throws Exception{
	    	CfLoginUtils.enterLoginCredentials(username);
	    }
	    
	    @Test(dependsOnMethods="enterLoginCredentials")
		  public void clickOnNext() throws Exception{
	    	CfLoginUtils.clickOnNext();
		    }
	    
	    @Test(dependsOnMethods="clickOnNext")
	    @Parameters("password")
		  public void enterLoginPassword(String password) throws Exception{
	    	CfLoginUtils.enterLoginPassword(password);
		    }
	    @Test(dependsOnMethods="enterLoginPassword")
		  public void clickOnNextPassword() throws Exception{
	    	CfLoginUtils.clickOnNextPassword();
		    }
	    
	    @Test(dependsOnMethods="clickOnNextPassword")
		public void checkPasscodeInfo() throws Exception{
	    	CfLoginUtils.checkPasscodeInfo();
	
		} 
	    
	    @Test(dependsOnMethods="checkPasscodeInfo")
		public void writePasscode() throws Exception{
	    	CfLoginUtils.writePasscode();
	
		} 
	    
	    
	    @Test(dependsOnMethods="writePasscode")
		public void openCmdRunCommand() throws Exception{
	    	CfLoginUtils.openCmdRunCommand();
	
		} 
	    
	    
//	    @Test(priority = 1)
//	    public void fetchPredixUsername() {
//	        String sqlQuery = "select predix_username from deploycloudfoundary_information WHERE job_id=1";
//	        predixUserName = DataBaseConnector.executeSQLQuery("QA", sqlQuery);
//	        System.out.println("Employee name retrieved from database :" + predixUserName);
//	    }
//	    @Test(priority = 1)
//	    public void fetchPredixPassword() {
//	        String sqlQuery = "select predix_password from deploycloudfoundary_information WHERE job_id=1";
//	        predixPassword = DataBaseConnector.executeSQLQuery("QA", sqlQuery);
//	        System.out.println("Employee name retrieved from database :" + predixPassword);
//	    }
}
