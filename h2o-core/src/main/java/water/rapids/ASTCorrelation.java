package water.rapids;

import water.Key;
import water.MRTask;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.ArrayUtils;

import java.util.Arrays;

/** Correlation between columns of a frame
 TODO Handle NA's properly...
 * */
class ASTCorrelation extends ASTPrim {
    @Override
    public String[] args() { return new String[]{"ary","x","y"}; }
    @Override int nargs() { return 1+2; /* (cor X Y) */}
    @Override public String str() { return "cor"; }
    @Override
    public Val apply(Env env, Env.StackHelp stk, AST asts[]) {
        Frame frx = stk.track(asts[1].exec(env)).getFrame();
        Frame fry = stk.track(asts[2].exec(env)).getFrame();
        if( frx.numRows() != fry.numRows() )
            throw new IllegalArgumentException("Frames must have the same number of rows, found "+frx.numRows()+" and "+fry.numRows());
        return fry.numRows() == 1 ? scalar(frx,fry) : array(frx,fry);
    }

    // Correlation for 1 row. This will return a scalar value.
    private ValNum scalar( Frame frx, Frame fry) {
        if( frx.numCols() != fry.numCols())
            throw new IllegalArgumentException("Single rows must have the same number of columns, found "+frx.numCols()+" and "+fry.numCols());
        Vec vecxs[] = frx.vecs();
        Vec vecys[] = fry.vecs();
        double xmean=0, ymean=0, xvar=0, yvar=0,xsd=0,ysd=0, ncols = frx.numCols(), NACount=0, xval, yval, ss=0;
        for( int r = 0; r < ncols; r++) {
            xval = vecxs[r].at(0);
            yval = vecys[r].at(0);
            if (Double.isNaN(xval) || Double.isNaN(yval))
                NACount++;
            else {
                xmean += xval;
                ymean += yval;
            }
        }
        xmean /= (ncols - NACount); ymean /= (ncols - NACount);

        for( int r = 0; r < ncols; r++ ) {
            xval = vecxs[r].at(0);
            yval = vecys[r].at(0);
            if (!(Double.isNaN(xval) || Double.isNaN(yval)))
                //Compute variance of x and y vars
                xvar += Math.pow((vecxs[r].at(0) - xmean), 2);
                yvar += Math.pow((vecys[r].at(0) - ymean), 2);
                //Compute sum of squares of x and y
                ss += (vecxs[r].at(0) - xmean) * (vecys[r].at(0) - ymean);
            }
        xsd = Math.sqrt(xvar/(frx.numRows())); //Sample Standard Deviation
        ysd = Math.sqrt(yvar/(fry.numRows())); //Sample Standard Deviation
        double cor_denom = xsd * ysd;
        return new ValNum(ss/cor_denom); //Pearson's Correlation Coefficient
    }

    // Correlation Matrix.
    // Compute correlation between all columns from each Frame against each other.
    // Return a matrix of correlations which is frx.numCols wide and fry.numCols tall.
    private Val array( Frame frx, Frame fry) {
        Vec[] vecxs = frx.vecs();
        int ncolx = vecxs.length;
        Vec[] vecys = fry.vecs();
        int ncoly = vecys.length;

        CorTaskEverything[] cvs = new CorTaskEverything[ncoly];

        double[] xmeans = new double[ncolx];
        for (int x = 0; x < ncolx; x++) {
            xmeans[x] = vecxs[x].mean();
        }

        // Launch tasks, which does all Xs vs one Y.
        for (int y = 0; y < ncoly; y++)
            cvs[y] = new CorTaskEverything(vecys[y].mean(),xmeans).dfork(new Frame(vecys[y]).add(frx));

        // 1 column will return a scalar
        if (ncolx == 1 && ncoly == 1) {
            return new ValNum(cvs[0].getResult()._cors[0]);
        }

        // Gather all the Xs-vs-Y correlation arrays and build out final Frame of correlations.
        Vec[] res = new Vec[ncoly];
        Key<Vec>[] keys = Vec.VectorGroup.VG_LEN1.addVecs(ncoly);
        for (int y = 0; y < ncoly; y++)
            res[y] = Vec.makeVec(cvs[y].getResult()._cors, keys[y]);

        return new ValFrame(new Frame(fry._names, res));
    }

    private static class CorTaskEverything extends MRTask<CorTaskEverything> {
        double[] _cors;
        final double _xmeans[], _ymean;
        CorTaskEverything(double ymean, double[] xmeans) { _ymean = ymean;_xmeans = xmeans; }
        @Override public void map( Chunk cs[] ) {
            final int ncolsx = cs.length-1;
            final Chunk cy = cs[0];
            final int len = cy._len;
            _cors = new double[ncolsx];
            double sum;
            double varx;
            double vary;
            for( int x=0; x<ncolsx; x++ ) {
                sum = 0;
                varx = 0;
                vary = 0;
                final Chunk cx = cs[x+1];
                final double xmean = _xmeans[x];
                for( int row=0; row<len; row++ ) {
                    varx += ((cx.atd(row) - xmean) * (cx.atd(row) - xmean))/(len-1); //Compute variance for x
                    vary += ((cy.atd(row) - _ymean) * (cy.atd(row) - _ymean))/(len-1); //Compute variance for y
                    sum += ((cx.atd(row) - xmean) * (cy.atd(row) - _ymean))/(len-1); //Compute sum of square
                }
                _cors[x] = sum/(Math.sqrt(varx) * Math.sqrt(vary)); //Pearsons correlation coefficient
            }
        }
        @Override public void reduce( CorTaskEverything cvt ) { ArrayUtils.add(_cors,cvt._cors); }
    }
}
