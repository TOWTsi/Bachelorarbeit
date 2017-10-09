package belsSWTplayground;

import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class SWTLabel {

	//Add Label to Shell
	/*public static void main(String[] args) {
	    Display display = new Display();
	    Shell shell = new Shell();
	    shell.setLayout(new GridLayout(1, false));

	    // Create a label
	    new Label(shell, SWT.NONE).setText("This is a plain label.");
	    new Label(shell, SWT.NONE).setText("And one more.");

	    shell.open();
	    while (!shell.isDisposed()) {
	      if (!display.readAndDispatch()) {
	        display.sleep();
	      }
	    }
	    display.dispose();
	  }*/
	
	
	//Use Label as a separator
	/*public static void main(String[] args) {
	    Display display = new Display();
	    Shell shell = new Shell();
	    shell.setLayout(new GridLayout(1, false));

	    // Create a vertical separator
	    new Label(shell, SWT.SEPARATOR);

	    shell.open();
	    while (!shell.isDisposed()) {
	      if (!display.readAndDispatch()) {
	        display.sleep();
	      }
	    }
	    display.dispose();
	  }*/
	
	
	//Label with border
	/*public static void main(String[] args) {
	    Display display = new Display();
	    Shell shell = new Shell();
	    shell.setLayout(new GridLayout(1, false));

	    // Create a label with a border
	    new Label(shell, SWT.BORDER).setText("This is a label with a border.");

	    shell.open();
	    while (!shell.isDisposed()) {
	      if (!display.readAndDispatch()) {
	        display.sleep();
	      }
	    }
	    display.dispose();
	  }*/
	
	
	//Horizontal Separator Label
	/*public static void main(String[] args) {
	    Display display = new Display();
	    Shell shell = new Shell();
	    shell.setLayout(new GridLayout(1, false));

	    // Create a horizontal separator
	    Label separator = new Label(shell, SWT.HORIZONTAL | SWT.SEPARATOR);
	    separator.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));

	    shell.open();
	    while (!shell.isDisposed()) {
	      if (!display.readAndDispatch()) {
	        display.sleep();
	      }
	    }
	    display.dispose();
	  }*/
	
}
