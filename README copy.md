combined_cacert.pem

This file contains one or more intermediate certificates concatenated with the system cert bundle (certifi). It is used to work around servers that do not provide the full certificate chain.

If you encounter 'unable to get local issuer certificate':
- Obtain the missing intermediate certificate (PEM) from the issuer (AIA URL in the leaf cert).
- Append the intermediate PEM to the certifi bundle:
  cat intermediate.pem "$(python -c 'import certifi; print(certifi.where())')" > combined_cacert.pem
- Place the resulting file here: `versions/poc-v-d33/config/combined_cacert.pem`
- Re-run the script.

Note: Prefer installing intermediates at system level (ca-certificates) for a long-term solution.