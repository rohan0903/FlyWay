package util;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.remote.RemoteWebDriver;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.remote.CapabilityType;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
public class CfLoginUtils {
	
	public static Actions action;
	public static WebDriver driver;
	public static WebDriverWait wait;
	public static JavascriptExecutor executor;
	
	static String passcode;
	static RemoteWebDriver remote;
	
	public static void setUp(String app_url) throws Exception {
		
		String UsrDir = System.getProperty("user.dir");
		
		System.setProperty("webdriver.chrome.driver", UsrDir+"//lib//chromedriver.exe");
		HashMap<String, Object> chromePrefs = new HashMap<String, Object>();
		chromePrefs.put("profile.default_content_settings.popups", 0); //?
		ChromeOptions options = new ChromeOptions();
		options.setExperimentalOption("prefs", chromePrefs);
		options.addArguments("disable-extensions");
		options.addArguments("--start-maximized");
		DesiredCapabilities cap = DesiredCapabilities.chrome();
		cap.setCapability(CapabilityType.ACCEPT_SSL_CERTS, true);
		cap.setCapability(ChromeOptions.CAPABILITY, options);
		driver = new ChromeDriver(cap);
		wait = new WebDriverWait(driver, 60);        
		driver.manage().timeouts().implicitlyWait(10, TimeUnit.SECONDS);	 	
		driver.manage().window().maximize();    
		executor=(JavascriptExecutor)driver;				
		driver.get(app_url); 
		
		
	}
	
	public static void waitForLoginPage() throws Exception {
		Thread.sleep(5000);
	}
	
	public static void enterLoginCredentials(String username) throws Exception {
		driver.findElement(By.id("email")).clear();
		driver.findElement(By.id("email")).sendKeys("predixUserName");
	}
	public static void clickOnNext() throws Exception {
		Thread.sleep(3000);
		driver.findElement(By.xpath("/html/body/div[1]/div/div/div[2]/form/div[2]/input")).click();
	}
	public static void enterLoginPassword(String password) throws Exception {
           Thread.sleep(3000);
		driver.findElement(By.id("password")).clear();
		driver.findElement(By.id("password")).sendKeys("predixPassword");
	}
	public static void clickOnNextPassword() throws Exception {
		Thread.sleep(3000);
		driver.findElement(By.xpath("/html/body/div[1]/div/div/div[2]/form/div[2]/input")).click();
	}
	
	public static void checkPasscodeInfo() throws Exception{
		Thread.sleep(500);	
		passcode=driver.findElement(By.className("island")).findElements(By.tagName("h2")).get(0).getText();
		System.out.println(passcode);
Thread.sleep(2000);
		
	} 
	
	public static void writePasscode() throws Exception{
	PrintWriter writer = new PrintWriter("C:/jenkins_99/workspace/CFPushPasscode_Selenium/src/test/java/test/test.txt");
	writer.println(passcode);
	writer.close();
	}
public static void openCmdRunCommand() throws Exception{
		String new_dir="C:/Program Files/PuTTY/";
		Runtime rt = Runtime.getRuntime();
		//rt.exec(new String[]{"cmd.exe","/c","start"});    to open cmd
		/*rt.exec("cmd.exe /c cd \""+new_dir+"\" & start cmd.exe /k \"java -flag -flag -cp terminal-based-program.jar\"");*/
		rt.exec("cmd.exe /c cd \""+new_dir+"\" & start cmd.exe /k \"pscp.exe -pw Igate@123 C:/jenkins_99/workspace/CFPushPasscode_Selenium/src/test/java/test/test.txt devops99@3.209.30.99:/home/devops99\"");
		
		}
	
	
	
}
