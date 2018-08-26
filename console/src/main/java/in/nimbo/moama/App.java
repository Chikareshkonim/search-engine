package in.nimbo.moama;

import asg.cliche.ShellFactory;
import in.nimbo.moama.console.Console;

import java.io.IOException;

public class App 
{
    public static void main( String[] args ) {
        try {
            ShellFactory.createConsoleShell("Console Search", "", new Console()).commandLoop();
            System.exit(0);
        } catch (IOException e) {
            System.out.println("Console failed to open");
        }
    }
}
