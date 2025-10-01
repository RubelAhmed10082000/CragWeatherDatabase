$max=5; $seen=@{}
git ls-files |
  ForEach-Object { $_ -replace '/','\' } |
  ForEach-Object {
    $parts = $_ -split '\\'
    $lim = [math]::Min($parts.Length, $max)
    for ($i=0; $i -lt $lim; $i++) {
      $path = ($parts[0..$i] -join '\')
      if (-not $seen.ContainsKey($path)) {
        $seen[$path] = $true
        $indent = '  ' * $i
        "$indent$($parts[$i])"
      }
    }
  } | Tee-Object repo-tree.txt