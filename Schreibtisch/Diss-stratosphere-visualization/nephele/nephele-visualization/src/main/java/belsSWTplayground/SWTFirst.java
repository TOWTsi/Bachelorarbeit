package belsSWTplayground;

import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;

public class SWTFirst {
  public static void main(String[] args) {
    Display display = new Display();
    Shell shell = new Shell(display);

    shell.setText("Hello, world!");

    shell.open();
    // Set up the event loop.
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        // If no more entries in the event queue
        display.sleep();
      }
    }
    display.dispose();
  }
}
