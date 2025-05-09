window.esmsInitOptions = {
  shimMode: true,
  fetch: function (url, options) {
    let doFetch = fetch(url, options);
    if (!url.endsWith('.html')) {
      return doFetch;
    }
    return doFetch
      .then(res => res.text())
      .then(result => {
        let blob = new Blob(
          ['export default ' + JSON.stringify(result)],
          {type: 'application/javascript'}
        );

        return new Response(blob);
      });
  }
};
