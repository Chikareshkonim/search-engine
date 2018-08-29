package in.nimbo.moama;

import asg.cliche.ShellFactory;
import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.console.Console;

import java.io.IOException;
import java.io.InputStream;

public class App
{
    public static void main( String[] args ) {
        InputStream fileInputStream = App.class.getResourceAsStream("/console.properties");
        ConfigManager configManager=ConfigManager.getInstance();
        try {
            configManager.load(fileInputStream,ConfigManager.FileType.PROPERTIES);
        } catch (IOException e) {
            System.out.println("Loading properties failed!");
        }
        try {
            ShellFactory.createConsoleShell("Console Search", "", new Console()).commandLoop();
            System.exit(0);
        } catch (IOException e) {
            System.out.println("Console failed to open");
        }
    }
}
